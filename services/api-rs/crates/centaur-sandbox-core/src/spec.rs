use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepoCacheAccess {
    None,
    Public,
    #[default]
    All,
}

impl RepoCacheAccess {
    pub fn enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Public => "public",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapabilities {
    #[serde(default)]
    pub repo_cache: RepoCacheAccess,
    pub observability_enabled: bool,
    pub api_server_enabled: bool,
}

impl SandboxCapabilities {
    pub const fn default_enabled() -> Self {
        Self {
            repo_cache: RepoCacheAccess::All,
            observability_enabled: true,
            api_server_enabled: true,
        }
    }

    pub fn is_default_enabled(&self) -> bool {
        self.repo_cache.enabled() && self.observability_enabled && self.api_server_enabled
    }
}

impl Default for SandboxCapabilities {
    fn default() -> Self {
        Self::default_enabled()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxSpec {
    pub image: String,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    pub command: Option<Vec<String>>,
    pub args: Vec<String>,
    pub env: Vec<EnvVar>,
    pub working_dir: Option<String>,
    pub mounts: Vec<Mount>,
    pub resources: Option<ResourceLimits>,
    /// iron-control principal OID (``prn_…``) this sandbox's egress proxy
    /// should act as. When set, the backend registers/binds an iron-control
    /// proxy for the sandbox instead of rendering a static proxy config.
    #[serde(default)]
    pub iron_control_principal: Option<String>,
    /// Labels applied to the iron-control proxy registered for this sandbox.
    /// These are distinct from Kubernetes labels and are used by iron-control
    /// when rendering proxy-specific config.
    #[serde(default)]
    pub iron_control_proxy_labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: SandboxCapabilities,
}

impl SandboxSpec {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            labels: std::collections::BTreeMap::new(),
            command: None,
            args: Vec::new(),
            env: Vec::new(),
            working_dir: None,
            mounts: Vec::new(),
            resources: None,
            iron_control_principal: None,
            iron_control_proxy_labels: std::collections::BTreeMap::new(),
            capabilities: SandboxCapabilities::default_enabled(),
        }
    }

    pub fn iron_control_principal(mut self, principal_foreign_id: impl Into<String>) -> Self {
        self.iron_control_principal = Some(principal_foreign_id.into());
        self
    }

    pub fn capabilities(mut self, capabilities: SandboxCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(name.into(), value.into());
        self
    }

    pub fn command(mut self, command: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.command = Some(command.into_iter().map(Into::into).collect());
        self
    }

    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Set an env var, replacing the value in place when the name is already
    /// present. Duplicate names must never reach a backend: the
    /// agents.x-k8s.io Sandbox CRD rejects them outright (FieldValueDuplicate),
    /// unlike plain Pod specs. Replacement is in place (not remove + push) so
    /// entry order — and therefore the serialized spec bytes that feed the
    /// warm-pool workload key — stays stable.
    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();
        if let Some(existing) = self.env.iter_mut().find(|env| env.name == name) {
            existing.value = value;
        } else {
            self.env.push(EnvVar::new(name, value));
        }
        self
    }

    pub fn working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    pub fn mount(mut self, mount: Mount) -> Self {
        self.mounts.push(mount);
        self
    }

    pub fn resources(mut self, resources: ResourceLimits) -> Self {
        self.resources = Some(resources);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

impl EnvVar {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    pub kind: MountKind,
    pub target_path: String,
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

impl Mount {
    pub fn new(kind: MountKind, target_path: impl Into<String>) -> Self {
        Self {
            kind,
            target_path: target_path.into(),
            read_only: false,
            sub_path: None,
        }
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn sub_path(mut self, sub_path: impl Into<String>) -> Self {
        self.sub_path = Some(sub_path.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MountKind {
    EmptyDir,
    NamedVolume(String),
    Bind { source_path: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub cpu_millis: Option<u32>,
    pub memory_bytes: Option<u64>,
}

impl ResourceLimits {
    pub fn new() -> Self {
        Self {
            cpu_millis: None,
            memory_bytes: None,
        }
    }

    pub fn cpu_millis(mut self, cpu_millis: u32) -> Self {
        self.cpu_millis = Some(cpu_millis);
        self
    }

    pub fn memory_bytes(mut self, memory_bytes: u64) -> Self {
        self.memory_bytes = Some(memory_bytes);
        self
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_pairs(spec: &SandboxSpec) -> Vec<(&str, &str)> {
        spec.env
            .iter()
            .map(|env| (env.name.as_str(), env.value.as_str()))
            .collect()
    }

    #[test]
    fn env_appends_new_names_in_order() {
        let spec = SandboxSpec::new("image")
            .env("A", "1")
            .env("B", "2")
            .env("C", "3");

        assert_eq!(env_pairs(&spec), [("A", "1"), ("B", "2"), ("C", "3")]);
    }

    #[test]
    fn env_replaces_existing_name_in_place() {
        // In-place replacement (index preserved) matters beyond dedupe: the
        // warm-pool workload key hashes the serialized spec, so reordering
        // entries would spuriously roll the pool.
        let spec = SandboxSpec::new("image")
            .env("A", "1")
            .env("B", "2")
            .env("C", "3")
            .env("B", "override");

        assert_eq!(
            env_pairs(&spec),
            [("A", "1"), ("B", "override"), ("C", "3")]
        );
    }

    #[test]
    fn env_never_yields_duplicate_names() {
        // The operator env template may pre-seed a placeholder (e.g.
        // CENTAUR_SANDBOX_MODEL_TOKEN) that a later caller overrides with a
        // real value; the agents.x-k8s.io Sandbox CRD rejects duplicate env
        // names, so the override must collapse to a single entry.
        let spec = SandboxSpec::new("image")
            .env("TOKEN", "placeholder")
            .env("TOKEN", "minted");

        assert_eq!(env_pairs(&spec), [("TOKEN", "minted")]);
    }
}
