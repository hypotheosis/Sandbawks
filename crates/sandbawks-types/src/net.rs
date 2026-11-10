//! Network address formatting shared across the controller and node.

use std::fmt::Display;

/// Join a host and port into a URI authority, bracketing IPv6 literals.
///
/// Kubernetes hands us `Pod.status.podIP` as a bare IP literal. IPv4
/// joins directly to `host:port`, but an IPv6 literal contains colons
/// and must be wrapped in brackets — `[host]:port` — to form a valid
/// authority (RFC 3986). Unbracketed, it is rejected by URI parsers
/// (`http://{addr}`), `SocketAddr`/`ToSocketAddrs`, and HTTP `Host`
/// headers, which on IPv6 clusters surfaces as misleading "invalid URI"
/// / sandbox-timeout failures.
///
/// We branch on the presence of a colon in `host` because every call
/// site passes a bare IP literal — never a scheme-bearing or
/// already-bracketed host — so a colon unambiguously marks the IPv6
/// case. `port` is generic over [`Display`] so callers can pass it as
/// `u16`, `i32` (matching `ContainerPort`), or any other integer type
/// without a cast.
///
/// # Examples
///
/// ```
/// use sandbawks_types::net::host_port;
///
/// assert_eq!(host_port("10.0.0.1", 49983), "10.0.0.1:49983");
/// assert_eq!(host_port("fdcd:4a83::70ac", 49983), "[fdcd:4a83::70ac]:49983");
/// ```
pub fn host_port<P: Display>(host: &str, port: P) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::host_port;

    #[test]
    fn ipv4_is_unbracketed() {
        assert_eq!(host_port("10.0.0.1", 49983_u16), "10.0.0.1:49983");
    }

    #[test]
    fn ipv6_is_bracketed() {
        // Reproduces the reported authority that an unbracketed join
        // turned into the invalid URI `fdcd:4a83:f883:5a::70ac:49983`.
        assert_eq!(
            host_port("fdcd:4a83:f883:5a::70ac", 49983_u16),
            "[fdcd:4a83:f883:5a::70ac]:49983"
        );
    }

    #[test]
    fn accepts_signed_port_without_cast() {
        // ContainerPort is `i32`; the generic `Display` bound lets the
        // controller pass it through unchanged.
        assert_eq!(host_port("10.0.0.1", 49983_i32), "10.0.0.1:49983");
    }
}
