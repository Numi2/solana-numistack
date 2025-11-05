use crate::config::ValidatedConfig;
use core_affinity::{get_core_ids, CoreId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, Default)]
struct CpuTopoEntry {
    pub _logical_id: usize,
    pub package_id: Option<usize>,
    pub core_id: Option<usize>,
    pub numa_node: Option<usize>,
}

#[cfg(target_os = "linux")]
fn read_usize_from(path: &std::path::Path) -> Option<usize> {
    use std::fs;
    let s = fs::read_to_string(path).ok()?;
    s.trim().parse::<usize>().ok()
}

#[cfg(target_os = "linux")]
fn topo_for_cpu(cpu: usize) -> CpuTopoEntry {
    use std::path::PathBuf;
    let mut e = CpuTopoEntry {
        _logical_id: cpu,
        package_id: None,
        core_id: None,
        numa_node: None,
    };
    let mut p = PathBuf::from("/sys/devices/system/cpu");
    p.push(format!("cpu{}", cpu));
    let mut t = p.clone();
    t.push("topology/physical_package_id");
    if let Some(v) = read_usize_from(&t) {
        e.package_id = Some(v);
    }
    let mut t2 = p.clone();
    t2.push("topology/core_id");
    if let Some(v) = read_usize_from(&t2) {
        e.core_id = Some(v);
    }
    // find NUMA node by scanning /sys/devices/system/node/nodeX/cpuY
    for node in 0..8usize {
        let mut npath = std::path::PathBuf::from("/sys/devices/system/node");
        npath.push(format!("node{}", node));
        let mut cpu_path = npath.clone();
        cpu_path.push(format!("cpu{}", cpu));
        if cpu_path.exists() {
            e.numa_node = Some(node);
            break;
        }
    }
    e
}

#[cfg(not(target_os = "linux"))]
fn topo_for_cpu(cpu: usize) -> CpuTopoEntry {
    CpuTopoEntry {
        _logical_id: cpu,
        package_id: None,
        core_id: None,
        numa_node: None,
    }
}

#[allow(unused_variables)]
pub fn select_writer_core_ids(cfg: &ValidatedConfig, writer_threads: usize) -> Vec<CoreId> {
    let cores = match get_core_ids() {
        Some(v) => v,
        None => return Vec::new(),
    };

    if writer_threads == 0 {
        return Vec::new();
    }

    // Build topology map
    let mut topo: BTreeMap<usize, CpuTopoEntry> = BTreeMap::new();
    for c in &cores {
        topo.insert(c.id, topo_for_cpu(c.id));
    }

    // Preferred: same NUMA as pin_core if provided, but a different physical core_id
    let mut candidates: Vec<&CoreId> = Vec::new();

    // Read producer core from config on Linux only; otherwise None
    let prod_core: Option<usize> = {
        #[cfg(target_os = "linux")]
        {
            cfg.pin_core
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    };

    if let Some(prod_core) = prod_core {
        let prod = topo.get(&prod_core).cloned();
        if let Some(prod) = prod {
            let prefer_node = prod.numa_node;
            let mut seen_core_ids: BTreeSet<(Option<usize>, Option<usize>)> = BTreeSet::new();
            for c in &cores {
                if c.id == prod_core {
                    continue;
                }
                if let Some(ent) = topo.get(&c.id) {
                    if prefer_node.is_some() && ent.numa_node != prefer_node {
                        continue;
                    }
                    // filter out hyperthread siblings: skip same (package_id, core_id)
                    let phys = (ent.package_id, ent.core_id);
                    if phys == (prod.package_id, prod.core_id) {
                        continue;
                    }
                    // de-duplicate physical cores: pick first logical per physical core
                    if seen_core_ids.insert(phys) {
                        candidates.push(c);
                    }
                }
            }
        }
    }

    // Fallbacks
    if candidates.is_empty() {
        for c in &cores {
            if prod_core.map(|pc| pc != c.id).unwrap_or(true) {
                candidates.push(c);
            }
        }
    }

    candidates
        .into_iter()
        .take(writer_threads)
        .map(|c| CoreId { id: c.id })
        .collect()
}
