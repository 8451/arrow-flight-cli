// use lazy_static::lazy_static;
// use oauth2::{AuthType, AuthUrl, ClientId, ClientSecret, CsrfToken, RedirectUrl, Scope, TokenUrl};
// use oauth2::basic::BasicClient;
// use crate::{CliError, ConnectionConfig};
//
// lazy_static! {
//     static ref azure_auth_url: String = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize".to_string();
//     static ref azure_token_url: String = "https://login.microsoftonline.com/common/oauth2/v2.0/token".to_string();
// }
//
// pub fn azure_authorization(cfg: ConnectionConfig) -> Result<ConnectionConfig, CliError> {
//     let client_id = ClientId::new(cfg.client_id);
//     let client_secret = ClientSecret::new(cfg.client_secret);
//     let auth_url = AuthUrl::new(azure_auth_url.to_string()).unwrap();
//     let token_url = TokenUrl::new(azure_token_url.to_string()).unwrap();
//     let client = BasicClient::new(
//         client_id,
//         Some(client_secret),
//         auth_url,
//         Some(token_url)
//     )
//         .set_auth_type(AuthType::RequestBody)
//         .set_redirect_uri(
//             RedirectUrl::new("http://0.0.0.0:8000".to_string()).expect("Invalid Redirect URL"),
//         );
//
//     let (authorize_url, csrf_state) = client
//         .authorize_url(CsrfToken::new_random)
//         .url();
//
//     println!(
//         "Open this URL in your browser:\n{}\n",
//         authorize_url.to_string()
//     );
// }