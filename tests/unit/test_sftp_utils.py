"""
Tests unitaires pour les utilitaires SFTP.
"""

import pytest
from electricore.etl.sources.sftp_enedis import mask_password_in_url


class TestMaskPasswordInUrl:
    """Tests pour la fonction mask_password_in_url."""

    def test_mask_basic_url(self):
        """Test du masquage sur une URL SFTP basique."""
        url = "sftp://user:password@host.com:22/path"
        expected = "sftp://user:****@host.com:22/path"
        assert mask_password_in_url(url) == expected

    def test_mask_complex_password(self):
        """Test du masquage avec un mot de passe complexe (caractères spéciaux)."""
        url = "sftp://edn_ro:Ax*jp15cwmVPT8BRYcfP@84.46.252.128:22/flux_enedis"
        expected = "sftp://edn_ro:****@84.46.252.128:22/flux_enedis"
        assert mask_password_in_url(url) == expected

    def test_mask_no_path(self):
        """Test du masquage sur une URL sans chemin."""
        url = "sftp://admin:secret123@server.org:2222"
        expected = "sftp://admin:****@server.org:2222"
        assert mask_password_in_url(url) == expected

    def test_mask_with_deep_path(self):
        """Test du masquage avec un chemin profond."""
        url = "sftp://test:mypass@example.com:22/very/deep/path/structure"
        expected = "sftp://test:****@example.com:22/very/deep/path/structure"
        assert mask_password_in_url(url) == expected

    def test_no_password_url(self):
        """Test avec une URL sans mot de passe (ne devrait pas être modifiée)."""
        url = "sftp://user@host.com:22/path"
        # L'URL reste inchangée car pas de pattern mot de passe
        assert mask_password_in_url(url) == url

    def test_non_sftp_url(self):
        """Test avec une URL non-SFTP (ne devrait pas être modifiée)."""
        url = "https://user:password@example.com/api"
        # L'URL reste inchangée car le pattern cherche spécifiquement sftp://
        assert mask_password_in_url(url) == url

    def test_empty_string(self):
        """Test avec une chaîne vide."""
        assert mask_password_in_url("") == ""

    def test_malformed_url(self):
        """Test avec une URL malformée."""
        url = "sftp://malformed"
        # L'URL reste inchangée car elle ne correspond pas au pattern
        assert mask_password_in_url(url) == url

    def test_password_with_colon(self):
        """Test avec un mot de passe contenant des deux-points."""
        url = "sftp://user:pass:word@host.com:22/path"
        expected = "sftp://user:****@host.com:22/path"
        assert mask_password_in_url(url) == expected

    def test_password_with_at_symbol(self):
        """Test avec un mot de passe contenant un @."""
        # Note: Techniquement invalide dans une URL, mais testons la robustesse
        url = "sftp://user:p@ss@host.com:22/path"
        # Le regex devrait s'arrêter au premier @
        expected = "sftp://user:****@ss@host.com:22/path"
        assert mask_password_in_url(url) == expected