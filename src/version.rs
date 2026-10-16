use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl ExVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.trim().split('.');

        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;

        Some(Self::new(major, minor, patch))
    }
}

impl fmt::Display for ExVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_release_versions() {
        assert_eq!(ExVersion::parse("0.21.1"), Some(ExVersion::new(0, 21, 1)));
        assert_eq!(ExVersion::parse("0.21.1.0"), Some(ExVersion::new(0, 21, 1)));
        assert_eq!(ExVersion::parse("0.21"), None);
        assert_eq!(ExVersion::parse("abc"), None);
        assert_eq!(ExVersion::new(0, 21, 1).to_string(), "0.21.1");
    }

    #[test]
    fn orders_numerically() {
        assert!(ExVersion::new(0, 21, 1) < ExVersion::new(0, 22, 0));
        assert!(ExVersion::new(0, 9, 9) < ExVersion::new(0, 10, 0));
    }
}
