#!/usr/bin/env bash
# Generate dev/test certificates for mTLS.
# Usage: bash scripts/gen-certs.sh [output-dir]
#
# Produces:
#   ca.pem / ca-key.pem        — Certificate Authority
#   server.pem / server-key.pem — Server cert (signed by CA)
#   client.pem / client-key.pem — Client cert (signed by CA)

set -euo pipefail

# Prevent Git-for-Windows from mangling /CN=... into C:/Program Files/Git/CN=...
export MSYS_NO_PATHCONV=1

OUT="${1:-certs}"
mkdir -p "$OUT"

# ── CA ──
openssl req -x509 -newkey rsa:2048 \
  -keyout "$OUT/ca-key.pem" -out "$OUT/ca.pem" \
  -days 365 -nodes -subj "/CN=hermes-ca"

# ── Server ──
openssl req -newkey rsa:2048 \
  -keyout "$OUT/server-key.pem" -out "$OUT/server.csr" \
  -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"

openssl x509 -req -in "$OUT/server.csr" \
  -CA "$OUT/ca.pem" -CAkey "$OUT/ca-key.pem" -CAcreateserial \
  -out "$OUT/server.pem" -days 365 \
  -copy_extensions copyall

# ── Client ──
openssl req -newkey rsa:2048 \
  -keyout "$OUT/client-key.pem" -out "$OUT/client.csr" \
  -nodes -subj "/CN=hermes-client"

openssl x509 -req -in "$OUT/client.csr" \
  -CA "$OUT/ca.pem" -CAkey "$OUT/ca-key.pem" -CAcreateserial \
  -out "$OUT/client.pem" -days 365

# ── Cleanup CSR files ──
rm -f "$OUT"/*.csr "$OUT"/*.srl

echo "Certificates generated in $OUT/"
echo "  Server: HERMES_TLS_CERT=$OUT/server.pem HERMES_TLS_KEY=$OUT/server-key.pem HERMES_TLS_CA=$OUT/ca.pem"
echo "  Client: ca=$OUT/ca.pem cert=$OUT/client.pem key=$OUT/client-key.pem"
