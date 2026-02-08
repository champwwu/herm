#!/usr/bin/env python3
"""
Generate self-signed SSL certificates for mock exchange testing.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("ERROR: cryptography library not found. Install with: pip install cryptography")
    sys.exit(1)


def generate_self_signed_cert(cert_file: str, key_file: str):
    """Generate a self-signed SSL certificate and private key."""
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    
    # Create certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Test"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"TestCity"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"MockExchange"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
    ])
    
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.utcnow()
    ).not_valid_after(
        datetime.utcnow() + timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.DNSName(u"localhost"),
            x509.DNSName(u"127.0.0.1"),
        ]),
        critical=False,
    ).sign(private_key, hashes.SHA256(), default_backend())
    
    # Write private key
    with open(key_file, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    # Write certificate
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    print(f"✓ Generated self-signed certificate: {cert_file}")
    print(f"✓ Generated private key: {key_file}")


if __name__ == "__main__":
    script_dir = Path(__file__).parent
    cert_file = script_dir / "server.crt"
    key_file = script_dir / "server.key"
    
    if cert_file.exists() and key_file.exists():
        print("SSL certificates already exist. Skipping generation.")
        print(f"  Certificate: {cert_file}")
        print(f"  Private key: {key_file}")
        sys.exit(0)
    
    print("Generating self-signed SSL certificates...")
    generate_self_signed_cert(str(cert_file), str(key_file))
    print("\nCertificates generated successfully!")
