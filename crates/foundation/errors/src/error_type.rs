/// Stable identifier for an error, suitable for use as a metrics label.
///
/// This extension trait is implemented automatically for every error enum that
/// derives [`strum::IntoStaticStr`], which provides the
/// `From<&T> for &'static str` conversion this trait relies on. It replaces the
/// hand-written `error_type` methods that used to be duplicated across the error
/// modules.
pub trait ErrorType {
    /// Snake-case error identifier, for example `config_missing`.
    fn error_type(&self) -> &'static str;
}

impl<T> ErrorType for T
where
    for<'a> &'a T: Into<&'static str>,
{
    fn error_type(&self) -> &'static str {
        self.into()
    }
}
