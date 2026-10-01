use crate::config::SecurityConfig;
use std::net::{IpAddr, Ipv4Addr};
use url::{Host, Url};

use super::get_security_config;

/// Validates a webhook URL for security (SSRF prevention).
/// Blocks internal IP ranges, localhost, and non-HTTP schemes.
/// Can be skipped in debug builds via configuration.
pub fn validate_webhook_url(url_str: &str) -> Result<(), String> {
    validate_webhook_url_with_config(url_str, &get_security_config())
}

/// Validates a webhook URL with an explicit security configuration.
///
/// This is the config-injected entry point behind [`validate_webhook_url`]
/// (which pulls the process-global config). It is `pub` so callers that already
/// hold a [`SecurityConfig`] — and tests that need to exercise the strict path
/// without touching the global `OnceLock` — can validate directly.
///
/// Note (Audit 2, A5): this is a **creation-time** check. IP-literal hosts are
/// vetted here against [`is_internal_ip`]; domain hosts are only checked against
/// the configured blocklists — the IP a domain *resolves* to is enforced at
/// delivery time by the DNS resolver in [`crate::action`] (anti-DNS-rebinding).
///
/// Allowlists win over every block rule: a host matching
/// `SecurityConfig::allowed_hostnames` passes as soon as the scheme is http(s), and
/// an IP literal inside `SecurityConfig::allowed_cidrs` skips the internal-range
/// check (the hostname blocklists still apply to it, since they match on the
/// textual host — e.g. `0.0.0.0`).
pub fn validate_webhook_url_with_config(
    url_str: &str,
    config: &SecurityConfig,
) -> Result<(), String> {
    // Skip SSRF validation if configured (e.g., in debug builds)
    if config.skip_ssrf_validation {
        // Still validate URL format even when skipping SSRF checks
        Url::parse(url_str).map_err(|e| format!("Invalid URL format: {}", e))?;
        return Ok(());
    }

    // Parse the URL
    let url = Url::parse(url_str).map_err(|e| format!("Invalid URL format: {}", e))?;

    // Only allow http and https schemes
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(format!(
                "URL scheme '{}' not allowed, must be http or https",
                scheme
            ));
        }
    }

    // Get the host (string form, used for the configurable blocklist checks so
    // their behaviour is preserved for every host kind — including IP literals).
    let host = url
        .host_str()
        .ok_or_else(|| "URL must have a host".to_string())?;

    let host_lower = host.to_lowercase();

    // Explicitly trusted hostname (internal service the operator vouches for):
    // nothing below applies, and the delivery-time resolver skips it as well.
    if is_allowed_hostname(&host_lower, config) {
        return Ok(());
    }

    // Check against configurable blocked hostnames
    for blocked in &config.blocked_hostnames {
        let blocked_lower = blocked.to_lowercase();
        if host_lower == blocked_lower || host_lower.ends_with(&format!(".{}", blocked_lower)) {
            return Err(format!(
                "URL host '{}' is not allowed (internal/reserved)",
                host
            ));
        }
    }

    // Check for internal IP ranges via the *parsed* host. `url.host()` returns a
    // `Host::Ipv6(Ipv6Addr)` with the brackets already stripped, so IPv6 literals
    // (e.g. `[::1]`, `[fd00::1]`, `[::ffff:10.0.0.1]`) are actually inspected —
    // `host_str().parse::<IpAddr>()` always failed on the bracketed form and let
    // them through in release (Audit 2, A5).
    let literal = match url.host() {
        Some(Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
        Some(Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
        _ => None,
    };
    if let Some(ip) = literal
        && is_internal_ip(&ip)
        && !is_allowed_ip(&ip, config)
    {
        return Err(format!(
            "URL points to internal IP address '{}' which is not allowed",
            ip
        ));
    }

    // Check against configurable blocked hostname suffixes
    for suffix in &config.blocked_hostname_suffixes {
        if host_lower.ends_with(suffix) {
            return Err(format!("URL host '{}' appears to be internal", host));
        }
    }

    Ok(())
}

/// Checks whether an IPv4 address is in a private/internal/reserved range.
fn is_internal_ipv4(ipv4: &Ipv4Addr) -> bool {
    let octets = ipv4.octets();
    // 10.0.0.0/8 (private)
    if octets[0] == 10 {
        return true;
    }
    // 172.16.0.0/12 (private)
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return true;
    }
    // 192.168.0.0/16 (private)
    if octets[0] == 192 && octets[1] == 168 {
        return true;
    }
    // 127.0.0.0/8 (loopback)
    if octets[0] == 127 {
        return true;
    }
    // 169.254.0.0/16 (link-local / cloud metadata)
    if octets[0] == 169 && octets[1] == 254 {
        return true;
    }
    // 0.0.0.0/8
    if octets[0] == 0 {
        return true;
    }
    false
}

/// Checks if an IP address is in a private/internal range.
///
/// IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) are unwrapped and checked as
/// IPv4, so `::ffff:10.0.0.1` is blocked (Audit 2, A5).
pub(crate) fn is_internal_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => is_internal_ipv4(ipv4),
        IpAddr::V6(ipv6) => {
            // IPv4-mapped (::ffff:a.b.c.d) — apply the IPv4 rules to the embedded address.
            if let Some(v4) = ipv6.to_ipv4_mapped() {
                return is_internal_ipv4(&v4);
            }
            // ::1 (loopback)
            if ipv6.is_loopback() {
                return true;
            }
            // fe80::/10 (link-local)
            let segments = ipv6.segments();
            if (segments[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // fc00::/7 (unique local)
            if (segments[0] & 0xfe00) == 0xfc00 {
                return true;
            }
            // :: (unspecified)
            if ipv6.is_unspecified() {
                return true;
            }
            false
        }
    }
}

/// Does `host_lower` (already lowercased) match an `allowed_hostnames` entry?
/// Entries are exact names, or leading-dot suffixes (`.svc.cluster.local`).
pub(crate) fn is_allowed_hostname(host_lower: &str, config: &SecurityConfig) -> bool {
    config.allowed_hostnames.iter().any(|entry| {
        let entry = entry.trim().to_lowercase();
        if entry.is_empty() {
            false
        } else if let Some(suffix) = entry.strip_prefix('.') {
            host_lower
                .strip_suffix(suffix)
                .is_some_and(|rest| rest.ends_with('.'))
        } else {
            host_lower == entry
        }
    })
}

/// Is `ip` inside one of the operator-trusted `allowed_cidrs` networks?
pub(crate) fn is_allowed_ip(ip: &IpAddr, config: &SecurityConfig) -> bool {
    config.allowed_cidrs.iter().any(|net| net.contains(ip))
}

/// Pure filter used by the delivery-time DNS resolver (Audit 2, A5, anti-rebinding):
/// given the IPs a hostname resolved to, returns `Err(offending_ip)` if **any**
/// is internal/reserved and not inside an allowed CIDR. A hostname that resolves
/// to a mix of public and internal IPs is rejected (the safe choice — an attacker
/// could otherwise smuggle an internal target alongside a public one).
pub(crate) fn check_resolved_ips(ips: &[IpAddr], config: &SecurityConfig) -> Result<(), IpAddr> {
    for ip in ips {
        if is_internal_ip(ip) && !is_allowed_ip(ip, config) {
            return Err(*ip);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a strict security config for testing SSRF protection.
    /// Always validates URLs regardless of debug/release mode.
    fn strict_security_config() -> SecurityConfig {
        SecurityConfig {
            skip_ssrf_validation: false,
            ..SecurityConfig::default()
        }
    }

    /// Helper to validate webhook URL with strict config for tests.
    fn validate_url_strict(url: &str) -> Result<(), String> {
        validate_webhook_url_with_config(url, &strict_security_config())
    }

    #[test]
    fn test_ssrf_localhost_blocked() {
        assert!(validate_url_strict("http://localhost/api").is_err());
        assert!(validate_url_strict("http://localhost:8080/api").is_err());
        assert!(validate_url_strict("https://localhost/api").is_err());
    }

    #[test]
    fn test_ssrf_loopback_ip_blocked() {
        assert!(validate_url_strict("http://127.0.0.1/api").is_err());
        assert!(validate_url_strict("http://127.0.0.1:8080/api").is_err());
        assert!(validate_url_strict("http://127.1.2.3/api").is_err());
    }

    #[test]
    fn test_ssrf_private_ip_10_blocked() {
        assert!(validate_url_strict("http://10.0.0.1/api").is_err());
        assert!(validate_url_strict("http://10.255.255.255/api").is_err());
    }

    #[test]
    fn test_ssrf_private_ip_172_blocked() {
        assert!(validate_url_strict("http://172.16.0.1/api").is_err());
        assert!(validate_url_strict("http://172.31.255.255/api").is_err());
        // 172.15.x.x and 172.32.x.x should be allowed (not in private range)
        assert!(validate_url_strict("http://172.15.0.1/api").is_ok());
        assert!(validate_url_strict("http://172.32.0.1/api").is_ok());
    }

    #[test]
    fn test_ssrf_private_ip_192_168_blocked() {
        assert!(validate_url_strict("http://192.168.0.1/api").is_err());
        assert!(validate_url_strict("http://192.168.255.255/api").is_err());
    }

    #[test]
    fn test_ssrf_cloud_metadata_blocked() {
        assert!(validate_url_strict("http://169.254.169.254/latest/meta-data/").is_err());
        assert!(
            validate_url_strict("http://metadata.google.internal/computeMetadata/v1/").is_err()
        );
    }

    #[test]
    fn test_ssrf_file_scheme_blocked() {
        assert!(validate_url_strict("file:///etc/passwd").is_err());
    }

    #[test]
    fn test_ssrf_internal_domains_blocked() {
        assert!(validate_url_strict("http://service.local/api").is_err());
        assert!(validate_url_strict("http://app.internal/api").is_err());
        assert!(validate_url_strict("http://host.localdomain/api").is_err());
    }

    #[test]
    fn test_valid_external_urls() {
        assert!(validate_url_strict("https://example.com/webhook").is_ok());
        assert!(validate_url_strict("https://api.github.com/repos").is_ok());
        assert!(validate_url_strict("http://httpbin.org/post").is_ok());
        assert!(validate_url_strict("https://8.8.8.8/api").is_ok());
    }

    #[test]
    fn test_skip_ssrf_validation_allows_localhost() {
        let config = SecurityConfig {
            skip_ssrf_validation: true,
            ..SecurityConfig::default()
        };
        // With skip_ssrf_validation=true, localhost should be allowed
        assert!(validate_webhook_url_with_config("http://localhost/api", &config).is_ok());
        assert!(validate_webhook_url_with_config("http://127.0.0.1/api", &config).is_ok());
        // But invalid URLs should still fail
        assert!(validate_webhook_url_with_config("not-a-url", &config).is_err());
    }

    #[test]
    fn test_ssrf_ipv6_literals_blocked() {
        // Audit 2, A5: IPv6 literals used to bypass validation entirely because
        // `host_str()` returns the bracketed form ("[::1]") which never parses as
        // an IpAddr. `url.host()` returns the parsed Ipv6Addr, so these are now caught.
        assert!(
            validate_url_strict("http://[::1]:8085/").is_err(),
            "IPv6 loopback must be blocked"
        );
        assert!(
            validate_url_strict("http://[fd00::1]/").is_err(),
            "IPv6 ULA (fc00::/7) must be blocked"
        );
        assert!(
            validate_url_strict("http://[fe80::1]/").is_err(),
            "IPv6 link-local (fe80::/10) must be blocked"
        );
        assert!(
            validate_url_strict("http://[::]/").is_err(),
            "IPv6 unspecified must be blocked"
        );
    }

    #[test]
    fn test_ssrf_ipv4_mapped_ipv6_blocked() {
        // ::ffff:a.b.c.d must be unwrapped and checked as IPv4.
        assert!(
            validate_url_strict("http://[::ffff:10.0.0.1]/").is_err(),
            "IPv4-mapped private address must be blocked"
        );
        assert!(
            validate_url_strict("http://[::ffff:127.0.0.1]/").is_err(),
            "IPv4-mapped loopback must be blocked"
        );
        assert!(
            validate_url_strict("http://[::ffff:169.254.169.254]/").is_err(),
            "IPv4-mapped cloud-metadata address must be blocked"
        );
    }

    #[test]
    fn test_ssrf_public_ipv6_allowed() {
        // A genuine public IPv6 (Cloudflare DNS) must pass.
        assert!(
            validate_url_strict("https://[2606:4700:4700::1111]/").is_ok(),
            "public IPv6 must be allowed"
        );
    }

    #[test]
    fn test_check_resolved_ips_filter() {
        use std::net::Ipv6Addr;
        let cfg = strict_security_config();
        let check_resolved_ips = |ips: &[IpAddr]| check_resolved_ips(ips, &cfg);
        // All-public → Ok.
        assert!(
            check_resolved_ips(&[
                IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
                IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            ])
            .is_ok()
        );
        // Any internal → Err with the offending IP.
        let internal = IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254));
        assert_eq!(
            check_resolved_ips(&[IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), internal]),
            Err(internal),
            "a mix of public + internal must be rejected on the internal IP"
        );
        // IPv6 loopback → Err.
        let v6_loop = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert_eq!(check_resolved_ips(&[v6_loop]), Err(v6_loop));
        // IPv4-mapped private → Err.
        let mapped: IpAddr = "::ffff:10.0.0.5".parse().unwrap();
        assert!(check_resolved_ips(&[mapped]).is_err());
        // Empty → Ok (nothing resolved is not this function's concern).
        assert!(check_resolved_ips(&[]).is_ok());
    }

    #[test]
    fn test_custom_blocked_hostnames() {
        let config = SecurityConfig {
            skip_ssrf_validation: false,
            blocked_hostnames: vec!["myblocked.com".to_string()],
            blocked_hostname_suffixes: vec![".blocked".to_string()],
            ..SecurityConfig::default()
        };
        // Custom blocked hostname
        assert!(validate_webhook_url_with_config("http://myblocked.com/api", &config).is_err());
        // Custom blocked suffix
        assert!(validate_webhook_url_with_config("http://service.blocked/api", &config).is_err());
        // Default blocked hostnames should not be blocked with custom config
        assert!(validate_webhook_url_with_config("http://localhost/api", &config).is_ok());
        // But internal IPs are still blocked (hardcoded check)
        assert!(validate_webhook_url_with_config("http://10.0.0.1/api", &config).is_err());
    }

    fn allowlisted_config(hosts: &[&str], cidrs: &[&str]) -> SecurityConfig {
        SecurityConfig {
            skip_ssrf_validation: false,
            allowed_hostnames: hosts.iter().map(|h| h.to_string()).collect(),
            allowed_cidrs: crate::config::parse_cidr_list("ALLOWED_CIDRS", &cidrs.join(","))
                .unwrap(),
            ..SecurityConfig::default()
        }
    }

    #[test]
    fn test_allowed_hostname_exact_and_suffix() {
        let cfg = allowlisted_config(&["backend", ".svc.cluster.local"], &[]);
        // Exact match, case-insensitive, beats the default blocklists (`.local`).
        assert!(validate_webhook_url_with_config("http://backend:8000/hook", &cfg).is_ok());
        assert!(validate_webhook_url_with_config("http://BACKEND/hook", &cfg).is_ok());
        assert!(validate_webhook_url_with_config("http://api.svc.cluster.local/h", &cfg).is_ok());
        // Suffix needs a dot boundary and is not an exact match.
        assert!(validate_webhook_url_with_config("http://svc.cluster.local/h", &cfg).is_err());
        assert!(validate_webhook_url_with_config("http://evilsvc.cluster.local/h", &cfg).is_err());
        // Blocked names and internal literals stay blocked.
        assert!(validate_webhook_url_with_config("http://localhost/hook", &cfg).is_err());
        assert!(validate_webhook_url_with_config("http://10.0.0.1/hook", &cfg).is_err());
        // Scheme is still enforced for allowlisted hosts.
        assert!(validate_webhook_url_with_config("ftp://backend/hook", &cfg).is_err());
    }

    #[test]
    fn test_allowed_cidr_permits_internal_literal() {
        let cfg = allowlisted_config(&[], &["10.42.0.0/16", "192.168.1.10"]);
        assert!(validate_webhook_url_with_config("http://10.42.3.4:8000/hook", &cfg).is_ok());
        assert!(validate_webhook_url_with_config("http://192.168.1.10/hook", &cfg).is_ok());
        // Outside the allowed networks: still internal, still blocked.
        assert!(validate_webhook_url_with_config("http://10.43.0.1/hook", &cfg).is_err());
        assert!(validate_webhook_url_with_config("http://192.168.1.11/hook", &cfg).is_err());
        assert!(validate_webhook_url_with_config("http://127.0.0.1/hook", &cfg).is_err());
        // An IPv4-mapped v6 literal inside an allowed v4 range is NOT matched by
        // a v4 network (ipnet compares families) — it stays blocked, which is the
        // conservative outcome.
        assert!(validate_webhook_url_with_config("http://[::ffff:10.42.0.1]/hook", &cfg).is_err());
        // CIDRs do not loosen the textual hostname blocklist.
        assert!(validate_webhook_url_with_config("http://localhost/hook", &cfg).is_err());
    }

    #[test]
    fn test_check_resolved_ips_honours_allowed_cidrs() {
        let cfg = allowlisted_config(&[], &["10.42.0.0/16"]);
        let inside = IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1));
        let outside = IpAddr::V4(Ipv4Addr::new(10, 43, 1, 1));
        assert!(check_resolved_ips(&[inside], &cfg).is_ok());
        assert!(check_resolved_ips(&[IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), inside], &cfg).is_ok());
        assert_eq!(check_resolved_ips(&[inside, outside], &cfg), Err(outside));
    }

    #[test]
    fn test_is_allowed_hostname_matching() {
        let cfg = allowlisted_config(&["Backend", ".Svc.Cluster.Local", " "], &[]);
        assert!(is_allowed_hostname("backend", &cfg));
        assert!(!is_allowed_hostname("backend.example.com", &cfg));
        assert!(is_allowed_hostname("a.b.svc.cluster.local", &cfg));
        assert!(!is_allowed_hostname("svc.cluster.local", &cfg));
        assert!(!is_allowed_hostname("", &cfg));
    }
}
