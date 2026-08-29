use std::collections::HashSet;

use rand::seq::SliceRandom;

// No I/O/0/1
const CODE_CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
pub const CODE_LEN: usize = 5;

pub fn generate_join_code(taken: &HashSet<String>) -> String {
    let mut rng = rand::thread_rng();

    loop {
        let code: String = (0..CODE_LEN)
            .map(|_| *CODE_CHARSET.choose(&mut rng).unwrap() as char)
            .collect();

        if !taken.contains(&code) {
            return code;
        }
    }
}

pub fn normalize_code(code: &str) -> String {
    code.trim().to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_code_uses_len_and_charset() {
        let code = generate_join_code(&HashSet::new());

        assert_eq!(code.len(), CODE_LEN);
        assert!(code.bytes().all(|c| CODE_CHARSET.contains(&c)));
    }

    #[test]
    fn generated_code_avoids_taken() {
        let mut taken = HashSet::new();

        for _ in 0..100 {
            let code = generate_join_code(&taken);

            assert!(!taken.contains(&code));
            taken.insert(code);
        }
    }

    #[test]
    fn normalize_trims_and_uppercases() {
        assert_eq!(normalize_code("  ab2cd3ef \n"), "AB2CD3EF");
    }
}
