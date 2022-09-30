// MIT License
//
// Copyright (c) 2022 84.51
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
mod azure;

use crate::errors::CliError;

use chrono::{DateTime, Duration, Utc};

use oauth2::{AccessToken, RefreshToken};
use serde::{Deserialize, Serialize};

use std::error::Error;
use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

#[derive(Serialize, Deserialize, Debug, clap::ValueEnum, Clone, PartialEq)]
pub enum IdentityProvider {
    /// Azure Active Directory
    Azure,
    /// UNIMPLEMENTED: GitHub OAuth2
    Github,
    /// UNIMPLEMENTED: Google Auth
    Google,
    /// Skip OAuth2
    None,
}

impl FromStr for IdentityProvider {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = &s.to_ascii_lowercase()[..];
        match s {
            "azure" => Ok(IdentityProvider::Azure),
            "github" => Ok(IdentityProvider::Github),
            "google" => Ok(IdentityProvider::Google),
            "none" => Ok(IdentityProvider::None),
            _ => {
                eprintln!("Invalid Identity Provider: {}", s);
                Err(CliError::new("Invalid Identity Provider".to_string()))
            }
        }
    }
}

pub async fn authorize(
    identity_provider: &IdentityProvider,
    scope: &String,
) -> Result<AccessToken, Box<dyn Error>> {
    let stored_token = check_stored_token(identity_provider)?;
    if let Some(access_token) = stored_token.0 {
        Ok(access_token)
    } else {
        let refresh_token = stored_token.1;
        match identity_provider {
            IdentityProvider::Azure => Ok(azure::azure_authorize(scope.to_string(), refresh_token).await?),
            _ => Err(Box::new(CliError::new(
                "Identity Provider not currently implemented!".to_string(),
            ))),
        }
    }
}

fn check_stored_token(
    provider: &IdentityProvider,
) -> Result<(Option<AccessToken>, Option<RefreshToken>), Box<dyn Error>> {
    let store: AuthStore = confy::load("flight-cli-auth")?;
    if !store.token.secret().to_string().eq("") && store.identity_provider == *provider {
        let current_time = Utc::now();
        if store.expiry_time > current_time {
            return Ok((Some(store.token), None));
        } else if !store.refresh_token.secret().eq("") {
            return Ok((None, Some(store.refresh_token)));
        }
    }
    Ok((None, None))
}

fn store_access_token(
    access_token: AccessToken,
    expires_in: Option<core::time::Duration>,
    refresh_token: Option<&RefreshToken>,
    id_provider: IdentityProvider,
) -> Result<(), Box<dyn Error>> {
    let expiry_time = if let Some(token_duration) = expires_in {
        let token_duration = Duration::from_std(token_duration)?;
        Utc::now() + token_duration
    } else {
        // If there isn't an expires_in, just set expiry time to now so it'll get new credentials/refresh next time
        Utc::now()
    };
    let new_store = if let Some(refresh_token) = refresh_token {
        AuthStore {
            token: access_token,
            expiry_time,
            identity_provider: id_provider,
            refresh_token: refresh_token.to_owned(),
        }
    } else {
        AuthStore {
            token: access_token,
            expiry_time,
            identity_provider: id_provider,
            refresh_token: RefreshToken::new("".to_string()),
        }
    };

    confy::store("flight-cli-auth", &new_store)?;
    Ok(())
}

pub struct OAuth2Interceptor {
    pub(crate) access_token: String,
}

impl Interceptor for OAuth2Interceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if !self.access_token.eq("") {
            let new_token: MetadataValue<_> =
                format!("Bearer {}", self.access_token).parse().unwrap();
            request.metadata_mut().insert("authorization", new_token);
        }
        Ok(request)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthStore {
    token: AccessToken,
    expiry_time: DateTime<Utc>,
    identity_provider: IdentityProvider,
    refresh_token: RefreshToken,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self {
            token: AccessToken::new("".to_string()),
            expiry_time: DateTime::<Utc>::MIN_UTC,
            identity_provider: IdentityProvider::None,
            refresh_token: RefreshToken::new("".to_string()),
        }
    }
}
