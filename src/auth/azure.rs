use crate::auth::store_access_token;
use crate::IdentityProvider;
use azure_identity::{authorization_code_flow, development};
use oauth2::{AccessToken, ClientId, RefreshToken, TokenResponse};
use std::env;
use std::error::Error;
use url::Url;

pub async fn azure_authorize(
    scope: String,
    refresh_token: Option<RefreshToken>,
) -> Result<AccessToken, Box<dyn Error>> {
    let client_id = ClientId::new(
        env::var("OAUTH_CLIENT_ID").expect("Missing CLIENT_ID environment variable."),
    );
    let tenant_id = env::var("AZURE_TENANT_ID").expect("Missing TENANT_ID environment variable.");
    if let Some(refresh_token) = refresh_token {
        let token = azure_refresh_authorization(&refresh_token, &client_id, &tenant_id).await;
        match token {
            Ok(token) => return Ok(token),
            Err(e) => eprintln!("{}", e),
        }
    }
    azure_browser_authorization(scope, client_id, tenant_id).await
}

async fn azure_browser_authorization(
    scope: String,
    client_id: ClientId,
    tenant_id: String,
) -> Result<AccessToken, Box<dyn Error>> {
    let scope = format!("{} offline_access", scope);

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
    let tr = c.exchange(code).await?;
    let access_token = AccessToken::new(tr.access_token().secret().to_string());
    store_access_token(
        &access_token,
        tr.expires_in(),
        tr.refresh_token(),
        IdentityProvider::Azure,
    )?;
    Ok(tr.access_token().to_owned())
}

async fn azure_refresh_authorization(
    refresh_token: &RefreshToken,
    client_id: &ClientId,
    tenant_id: &String,
) -> Result<AccessToken, Box<dyn Error>> {
    let refresh_token = azure_core::auth::AccessToken::new(refresh_token.secret().to_string());
    let client = azure_core::new_http_client();
    let tr = azure_identity::refresh_token::exchange(
        client,
        &*tenant_id,
        client_id,
        None,
        &refresh_token,
    )
    .await?;
    let refresh_token = RefreshToken::new(tr.refresh_token().secret().to_string());
    let expires_in = std::time::Duration::from_secs(tr.expires_in());
    let access_token = AccessToken::new(tr.access_token().secret().to_string());
    store_access_token(
        &access_token,
        Some(expires_in),
        Some(&refresh_token),
        IdentityProvider::Azure,
    )?;
    Ok(access_token)
}
