"""Tests for secret validation functionality of LakehousePlumber."""

import pytest
from lhp.core.secret_validator import SecretValidator
from lhp.utils.substitution import SecretReference


class TestSecretValidator:
    """Test secret validation."""
    
    def test_validate_secret_references(self):
        """Test validating secret references."""
        validator = SecretValidator(available_scopes={'prod_secrets', 'dev_secrets'})
        
        refs = {
            SecretReference('prod_secrets', 'db_password'),
            SecretReference('dev_secrets', 'api-key'),
            SecretReference('unknown_scope', 'some_key')
        }
        
        errors = validator.validate_secret_references(refs)
        
        assert len(errors) == 1
        assert "unknown_scope" in errors[0]
    
    def test_key_format_validation(self):
        """Test secret key format validation."""
        validator = SecretValidator()
        
        # Valid formats
        assert validator._is_valid_key_format('db_password')
        assert validator._is_valid_key_format('api-key-123')
        assert validator._is_valid_key_format('TOKEN123')
        
        # Invalid formats
        assert not validator._is_valid_key_format('db password')
        assert not validator._is_valid_key_format('key@123')
        assert not validator._is_valid_key_format('key!value')


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 