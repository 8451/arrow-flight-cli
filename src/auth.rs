use crate::errors::CliError;
use azure_identity::{authorization_code_flow, development};
use oauth2::{AccessToken, ClientId, TokenResponse};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::str::FromStr;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};
use url::Url;

#[derive(Serialize, Deserialize, Debug)]
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

pub async fn azure_authorization() -> Result<AccessToken, Box<dyn Error>> {
    let client_id = ClientId::new(
        env::var("OAUTH_CLIENT_ID").expect("Missing CLIENT_ID environment variable."),
    );
    let tenant_id = env::var("AZURE_TENANT_ID").expect("Missing TENANT_ID environment variable.");
    let c = authorization_code_flow::start(
        client_id,
        None,
        &tenant_id,
        Url::parse("http://localhost:47471")?,
        "",
    );
    println!("\nOpen this URL in a browser:\n{}", c.authorize_url);

    // Using a naive, blocking redirect server is fine for our case, as it should block anyway
    let code = development::naive_redirect_server(&c, 47471)?;

    // Exchange the token with one that can be used for authorization
    let token = c.exchange(code).await?;
    Ok(token.access_token().to_owned())
}

pub struct OAuth2Interceptor {
    pub(crate) access_token: String,
}

impl Interceptor for OAuth2Interceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if self.access_token != "" {
            let new_token: MetadataValue<_> = format!("Bearer {}", self.access_token).parse().unwrap();
            request
                .metadata_mut()
                .insert("authorization", new_token.clone());
        }
        Ok(request)
    }
}
