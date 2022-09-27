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

use crate::errors::CliError;
use azure_identity::{authorization_code_flow, development};
use chrono::{DateTime, Utc};
use oauth2::basic::BasicTokenType;
use oauth2::{AccessToken, ClientId, EmptyExtraTokenFields, StandardTokenResponse, TokenResponse};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};
use url::Url;

#[derive(Serialize, Deserialize, Debug, clap::ValueEnum, Clone, PartialEq)]
pub enum IdentityProvider {
    /// Azure Active Directory
    Azure,
    /// UNIMPLEMENTED: GitHub OAuth2
    Github,
    /// UNIMPLEMENTED: Google Auth
    Google,
    /// Do not use OAuth2
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

pub async fn azure_authorization(scope: String) -> Result<AccessToken, Box<dyn Error>> {
    let stored_token = check_stored_token(IdentityProvider::Azure)?;
    if let Some(token) = stored_token {
        Ok(token)
    } else {
        let client_id = ClientId::new(
            env::var("OAUTH_CLIENT_ID").expect("Missing CLIENT_ID environment variable."),
        );
        let tenant_id =
            env::var("AZURE_TENANT_ID").expect("Missing TENANT_ID environment variable.");
        let c = authorization_code_flow::start(
            client_id,
            None,
            &tenant_id,
            Url::parse("http://localhost:47471")?,
            &*scope,
        );
        println!("\nOpen this URL in a browser:\n{}", c.authorize_url);

        // Using a naive, blocking redirect server is fine for our case, as it should block anyway
        let code = development::naive_redirect_server(&c, 47471)?;

        // Exchange the token with one that can be used for authorization
        let token = c.exchange(code).await?;
        store_access_token(&token, IdentityProvider::Azure)?;
        Ok(token.access_token().to_owned())
    }
}

fn check_stored_token(provider: IdentityProvider) -> Result<Option<AccessToken>, Box<dyn Error>> {
    let store: AuthStore = confy::load("flight-cli-auth")?;
    if !store.token.secret().to_string().eq("") && store.identity_provider == provider {
        let current_time = Utc::now();
        if store.expiry_time > current_time {
            return Ok(Some(store.token));
        }
    }
    Ok(None)
}

fn store_access_token(
    token_response: &StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>,
    id_provider: IdentityProvider,
) -> Result<(), Box<dyn Error>> {
    let expires_in = token_response.expires_in();
    let expiry_time = if let Some(token_duration) = expires_in {
        let token_duration = chrono::Duration::from_std(token_duration)?;
        Utc::now() + token_duration
    } else {
        // If there isn't an expires_in, just set expiry time to now so it'll get new credentials next time
        Utc::now()
    };
    let new_store = AuthStore {
        token: token_response.access_token().to_owned(),
        expiry_time,
        identity_provider: id_provider,
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
}

impl Default for AuthStore {
    fn default() -> Self {
        Self {
            token: AccessToken::new("".to_string()),
            expiry_time: DateTime::<Utc>::MIN_UTC,
            identity_provider: IdentityProvider::None,
        }
    }
}
