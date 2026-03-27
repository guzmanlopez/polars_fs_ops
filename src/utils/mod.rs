use std::path::Path;

pub fn check_file_to_file(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            let from_is_file = Path::new(from).is_file();
            from_is_file && has_valid_parent_dir(Some(to))
        },
        _ => false,
    }
}

pub fn check_valid_mv(
    from_path: Option<&str>,
    to_path: Option<&str>,
    preserve_extension: bool,
) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            if check_file_to_file(Some(from), Some(to)) {
                if preserve_extension && !has_same_ext(from_path, to_path) {
                    return false;
                }
                true
            } else {
                let from_is_dir = Path::new(from).is_dir();
                let to_is_not_file = !Path::new(to).is_file();
                from_is_dir && to_is_not_file && is_valid_dir_path_to_create(Some(to))
            }
        },
        _ => false,
    }
}

pub fn check_file_to_dir(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            let from_is_file = Path::new(from).is_file();
            let to_is_dir = Path::new(to).is_dir();
            from_is_file && to_is_dir
        },
        _ => false,
    }
}

pub fn is_valid_dir_path_to_create(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);

            if path.exists() {
                return path.is_dir();
            }

            match path.parent() {
                Some(parent) if parent.as_os_str().is_empty() => Path::new(".").is_dir(),
                Some(parent) => parent.is_dir(),
                None => false,
            }
        },
        _ => false,
    }
}

pub fn has_valid_parent_dir(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);
            match path.parent() {
                Some(parent) if parent.as_os_str().is_empty() => Path::new(".").is_dir(),
                Some(parent) => parent.is_dir(),
                None => false,
            }
        },
        _ => false,
    }
}

pub fn has_same_ext(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) if !to.is_empty() => {
            let from_path = Path::new(from);
            let to_path = Path::new(to);

            if let (Some(from_ext), Some(to_ext)) = (from_path.extension(), to_path.extension()) {
                from_ext == to_ext
            } else {
                false
            }
        },
        _ => false,
    }
}
