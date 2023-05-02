use crate::config::get_config;
use crate::errors::Error;
use arc_swap::ArcSwap;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::time::{sleep, Duration};
use trust_dns_resolver::error::{ResolveError, ResolveResult};
use trust_dns_resolver::lookup_ip::LookupIp;
use trust_dns_resolver::TokioAsyncResolver;

/// Cached Resolver Globally available
pub static CACHED_RESOLVER: Lazy<ArcSwap<CachedResolver>> =
    Lazy::new(|| ArcSwap::from_pointee(CachedResolver::default()));

// Ip addressed are returned as a set of addresses
// so we can compare.
#[derive(Clone, PartialEq, Debug)]
pub struct AddrSet {
    set: HashSet<IpAddr>,
}

impl AddrSet {
    fn new() -> AddrSet {
        AddrSet {
            set: HashSet::new(),
        }
    }
}

impl From<LookupIp> for AddrSet {
    fn from(lookup_ip: LookupIp) -> Self {
        let mut addr_set = AddrSet::new();
        for address in lookup_ip.iter() {
            addr_set.set.insert(address);
        }
        addr_set
    }
}

///
/// A CachedResolver is a DNS resolution cache mechanism with customizable expiration time.
///
/// The system works as follows:
///
/// When a host is to be resolved, if we have not resolved it before, a new resolution is
/// executed and stored in the internal cache. Concurrently, every `dns_max_ttl` time, the
/// cache is refreshed.
///
/// # Example:
///
/// ```
/// use pgcat::dns_cache::{CachedResolverConfig, CachedResolver};
///
/// # tokio_test::block_on(async {
/// let config = CachedResolverConfig::default();
/// let resolver = CachedResolver::new(config, None).await.unwrap();
/// let addrset = resolver.lookup_ip("www.example.com.").await.unwrap();
/// # })
/// ```
///
/// // Now the ip resolution is stored in local cache and subsequent
/// // calls will be returned from cache. Also, the cache is refreshed
/// // and updated every 10 seconds.
///
/// // You can now check if an 'old' lookup differs from what it's currently
/// // store in cache by using `has_changed`.
/// resolver.has_changed("www.example.com.", addrset)
#[derive(Default)]
pub struct CachedResolver {
    // The configuration of the cached_resolver.
    config: CachedResolverConfig,

    // This is the hash that contains the hash.
    data: Option<RwLock<HashMap<String, AddrSet>>>,

    // The resolver to be used for DNS queries.
    resolver: Option<TokioAsyncResolver>,

    // The RefreshLoop
    refresh_loop: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

///
/// Configuration
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CachedResolverConfig {
    /// Amount of time in secods that a resolved dns address is considered stale.
    dns_max_ttl: u64,

    /// Enabled or disabled? (this is so we can reload config)
    enabled: bool,
}

impl CachedResolverConfig {
    fn new(dns_max_ttl: u64, enabled: bool) -> Self {
        CachedResolverConfig {
            dns_max_ttl,
            enabled,
        }
    }
}

impl From<crate::config::Config> for CachedResolverConfig {
    fn from(config: crate::config::Config) -> Self {
        CachedResolverConfig::new(config.general.dns_max_ttl, config.general.dns_cache_enabled)
    }
}

impl CachedResolver {
    ///
    /// Returns a new Arc<CachedResolver> based on passed configuration.
    /// It also starts the loop that will refresh cache entries.
    ///
    /// # Arguments:
    ///
    /// * `config` - The `CachedResolverConfig` to be used to create the resolver.
    ///
    /// # Example:
    ///
    /// ```
    /// use pgcat::dns_cache::{CachedResolverConfig, CachedResolver};
    ///
    /// # tokio_test::block_on(async {
    /// let config = CachedResolverConfig::default();
    /// let resolver = CachedResolver::new(config, None).await.unwrap();
    /// # })
    /// ```
    ///
    pub async fn new(
        config: CachedResolverConfig,
        data: Option<HashMap<String, AddrSet>>,
    ) -> Result<Arc<Self>, io::Error> {
        // Construct a new Resolver with default configuration options
        let resolver = Some(TokioAsyncResolver::tokio_from_system_conf()?);

        let data = if let Some(hash) = data {
            Some(RwLock::new(hash))
        } else {
            Some(RwLock::new(HashMap::new()))
        };

        let instance = Arc::new(Self {
            config,
            resolver,
            data,
            refresh_loop: RwLock::new(None),
        });

        if instance.enabled() {
            info!("Scheduling DNS refresh loop");
            let refresh_loop = tokio::task::spawn({
                let instance = instance.clone();
                async move {
                    instance.refresh_dns_entries_loop().await;
                }
            });
            *(instance.refresh_loop.write().unwrap()) = Some(refresh_loop);
        }

        Ok(instance)
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    // Schedules the refresher
    async fn refresh_dns_entries_loop(&self) {
        let resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();
        let interval = Duration::from_secs(self.config.dns_max_ttl);
        loop {
            debug!("Begin refreshing cached DNS addresses.");
            // To minimize the time we hold the lock, we first create
            // an array with keys.
            let mut hostnames: Vec<String> = Vec::new();
            {
                if let Some(ref data) = self.data {
                    for hostname in data.read().unwrap().keys() {
                        hostnames.push(hostname.clone());
                    }
                }
            }

            for hostname in hostnames.iter() {
                let addrset = self
                    .fetch_from_cache(hostname.as_str())
                    .expect("Could not obtain expected address from cache, this should not happen");

                match resolver.lookup_ip(hostname).await {
                    Ok(lookup_ip) => {
                        let new_addrset = AddrSet::from(lookup_ip);
                        debug!(
                            "Obtained address for host ({}) -> ({:?})",
                            hostname, new_addrset
                        );

                        if addrset != new_addrset {
                            debug!(
                                "Addr changed from {:?} to {:?} updating cache.",
                                addrset, new_addrset
                            );
                            self.store_in_cache(hostname, new_addrset);
                        }
                    }
                    Err(err) => {
                        error!(
                            "There was an error trying to resolv {}: ({}).",
                            hostname, err
                        );
                    }
                }
            }
            debug!("Finished refreshing cached DNS addresses.");
            sleep(interval).await;
        }
    }

    /// Returns a `AddrSet` given the specified hostname.
    ///
    /// This method first tries to fetch the value from the cache, if it misses
    /// then it is resolved and stored in the cache. TTL from records is ignored.
    ///
    /// # Arguments
    ///
    /// * `host`      - A string slice referencing the hostname to be resolved.
    ///
    /// # Example:
    ///
    /// ```
    /// use pgcat::dns_cache::{CachedResolverConfig, CachedResolver};
    ///
    /// # tokio_test::block_on(async {
    /// let config = CachedResolverConfig::default();
    /// let resolver = CachedResolver::new(config, None).await.unwrap();
    /// let response = resolver.lookup_ip("www.google.com.");
    /// # })
    /// ```
    ///
    pub async fn lookup_ip(&self, host: &str) -> ResolveResult<AddrSet> {
        debug!("Lookup up {} in cache", host);
        match self.fetch_from_cache(host) {
            Some(addr_set) => {
                debug!("Cache hit!");
                Ok(addr_set)
            }
            None => {
                debug!("Not found, executing a dns query!");
                if let Some(ref resolver) = self.resolver {
                    let addr_set = AddrSet::from(resolver.lookup_ip(host).await?);
                    debug!("Obtained: {:?}", addr_set);
                    self.store_in_cache(host, addr_set.clone());
                    Ok(addr_set)
                } else {
                    Err(ResolveError::from("No resolver available"))
                }
            }
        }
    }

    //
    // Returns true if the stored host resolution differs from the AddrSet passed.
    pub fn has_changed(&self, host: &str, addr_set: &AddrSet) -> bool {
        if let Some(fetched_addr_set) = self.fetch_from_cache(host) {
            return fetched_addr_set != *addr_set;
        }
        false
    }

    // Fetches an AddrSet from the inner cache adquiring the read lock.
    fn fetch_from_cache(&self, key: &str) -> Option<AddrSet> {
        if let Some(ref hash) = self.data {
            if let Some(addr_set) = hash.read().unwrap().get(key) {
                return Some(addr_set.clone());
            }
        }
        None
    }

    // Sets up the global CACHED_RESOLVER static variable so we can globally use DNS
    // cache.
    pub async fn from_config() -> Result<(), Error> {
        let cached_resolver = CACHED_RESOLVER.load();
        let desired_config = CachedResolverConfig::from(get_config());

        if cached_resolver.config != desired_config {
            if let Some(ref refresh_loop) = *(cached_resolver.refresh_loop.write().unwrap()) {
                warn!("Killing Dnscache refresh loop as its configuration is being reloaded");
                refresh_loop.abort()
            }
            let new_resolver = if let Some(ref data) = cached_resolver.data {
                let data = Some(data.read().unwrap().clone());
                CachedResolver::new(desired_config, data).await
            } else {
                CachedResolver::new(desired_config, None).await
            };

            match new_resolver {
                Ok(ok) => {
                    CACHED_RESOLVER.store(ok);
                    Ok(())
                }
                Err(err) => {
                    let message = format!("Error setting up cached_resolver. Error: {:?}, will continue without this feature.", err);
                    Err(Error::DNSCachedError(message))
                }
            }
        } else {
            Ok(())
        }
    }

    // Stores the AddrSet in cache adquiring the write lock.
    fn store_in_cache(&self, host: &str, addr_set: AddrSet) {
        if let Some(ref data) = self.data {
            data.write().unwrap().insert(host.to_string(), addr_set);
        } else {
            error!("Could not insert, Hash not initialized");
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use trust_dns_resolver::error::ResolveError;

    #[tokio::test]
    async fn new() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await;
        assert!(resolver.is_ok());
    }

    #[tokio::test]
    async fn lookup_ip() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await.unwrap();
        let response = resolver.lookup_ip("www.google.com.").await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn has_changed() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await.unwrap();
        let hostname = "www.google.com.";
        let response = resolver.lookup_ip(hostname).await;
        let addr_set = response.unwrap();
        assert!(!resolver.has_changed(hostname, &addr_set));
    }

    #[tokio::test]
    async fn unknown_host() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await.unwrap();
        let hostname = "www.idontexists.";
        let response = resolver.lookup_ip(hostname).await;
        assert!(matches!(response, Err(ResolveError { .. })));
    }

    #[tokio::test]
    async fn incorrect_address() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await.unwrap();
        let hostname = "w  ww.idontexists.";
        let response = resolver.lookup_ip(hostname).await;
        assert!(matches!(response, Err(ResolveError { .. })));
        assert!(!resolver.has_changed(hostname, &AddrSet::new()));
    }

    #[tokio::test]
    // Ok, this test is based on the fact that google does DNS RR
    // and does not responds with every available ip everytime, so
    // if I cache here, it will miss after one cache iteration or two.
    async fn thread() {
        let config = CachedResolverConfig {
            dns_max_ttl: 10,
            enabled: true,
        };
        let resolver = CachedResolver::new(config, None).await.unwrap();
        let hostname = "www.google.com.";
        let response = resolver.lookup_ip(hostname).await;
        let addr_set = response.unwrap();
        assert!(!resolver.has_changed(hostname, &addr_set));
        let resolver_for_refresher = resolver.clone();
        let _thread_handle = tokio::task::spawn(async move {
            resolver_for_refresher.refresh_dns_entries_loop().await;
        });
        assert!(!resolver.has_changed(hostname, &addr_set));
    }
}
