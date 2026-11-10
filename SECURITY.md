# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.7.x   | :white_check_mark: |
| < 0.7.0 | :x:                |

## Reporting a Vulnerability

`pg-interleave` is intended strictly for isolated, local testing and CI environments. It terminates SSL/TLS handshakes to inspect plaintext wire-protocol frames and does not enforce authentication on its HTTP control port (`:9090`). **Do not expose pg-interleave to public networks or production databases.**

If you discover a security vulnerability or protocol flaw, please do not open a public issue. Send details privately via email to:

`ismail.yusifli86@gmail.com` (or open a private security advisory on GitHub).
