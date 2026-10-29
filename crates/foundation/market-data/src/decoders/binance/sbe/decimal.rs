use platform_errors::IngesterError;
use rust_decimal::Decimal;

pub(super) fn decimal_from_mantissa(mantissa: i64, exponent: i8) -> Result<Decimal, IngesterError> {
    let value = if exponent <= 0 {
        let scale = u32::from(exponent.unsigned_abs());
        Decimal::try_from_i128_with_scale(i128::from(mantissa), scale)
    } else {
        let factor = 10_i128
            .checked_pow(u32::from(exponent as u8))
            .ok_or_else(|| IngesterError::malformed_sbe("decimal exponent overflow"))?;
        let value = i128::from(mantissa)
            .checked_mul(factor)
            .ok_or_else(|| IngesterError::malformed_sbe("decimal mantissa overflow"))?;
        Decimal::try_from_i128_with_scale(value, 0)
    };
    value.map_err(|error| {
        IngesterError::malformed_sbe(format!(
            "invalid decimal mantissa {mantissa} with exponent {exponent}: {error}"
        ))
    })
}
