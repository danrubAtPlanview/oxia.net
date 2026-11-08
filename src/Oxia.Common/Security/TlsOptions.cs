// =====================================================================================================================
// PORT NOTE
//   Upstream : common/security/tls.go   (TLSOptions, TryIntoClientTLSConf, TryIntoServerTLSConf, makeCommonConfig, trustedCertPool,
//              validateKeyPair)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Turns the declarative TLS options (the YAML/JSON config of Oxia: cert/key files, CA bundle, versions, cipher suites,
//              server name, insecure-skip-verify, client auth) into the TLS configuration of a client or of a server.  Rules kept
//              from Go: TLS is on when Enabled is true, or when Enabled is unset and a CertFile is configured; the minimum version
//              defaults to TLS 1.2 and may not be lower; the maximum may not be below the minimum; the client certificate is
//              optional (only a server that asks for one gets it) and is re-read from disk at every handshake; a server needs
//              both files; a trusted CA file replaces the system roots and must hold at least one valid certificate; the server
//              requests (and verifies) client certificates when a CA file is set, and requires them when ClientAuth is set.
//   Divergences:
//      * tls.Config -> SslClientAuthenticationOptions / SslServerAuthenticationOptions.  Go's Min/MaxVersion (uint16 wire codes,
//        constants VersionTls1x here) become SslProtocols (Tls12|Tls13 range); only TLS 1.2 and 1.3 exist for 1.2+.
//      * RootCAs / ClientCAs -> a custom RemoteCertificateValidationCallback that builds the chain against
//        X509ChainTrustMode.CustomRootTrust (no revocation checks, like Go); the host name is still verified unless
//        InsecureSkipVerify (SslPolicyErrors.RemoteCertificateNameMismatch).  A server with a CA file but no ClientAuth
//        (Go: VerifyClientCertIfGiven) asks for a certificate and accepts the handshake without one.
//      * GetClientCertificate / GetCertificate -> LocalCertificateSelectionCallback / ServerCertificateSelectionCallback; the
//        PEM pair is loaded with X509Certificate2.CreateFromPemFile and round-tripped through PKCS#12 so the private key is
//        usable by SChannel (ephemeral keys are not).
//      * CipherSuites (IANA codes, like Go's []uint16) -> CipherSuitesPolicy, which .NET only supports on Linux / macOS: on other
//        platforms asking for cipher suites throws PlatformNotSupportedException instead of silently weakening the policy.
//      * (conf, error) -> the conf, throwing on error (OxiaException for the Go errors.New / Errorf cases, IOException for
//        unreadable files, CryptographicException for unusable key pairs).  Sentinels ErrInvalidTlsCertFile / ErrInvalidTlsKeyFile
//        are matched with OxiaErrors.Is.
//      * slog.Warn -> ILogger (the optional parameter of TryIntoServerTlsConf; default OxiaLog "tls").
//      * Struct tags (yaml/json/jsonschema), WithDefault and Validate: the properties are plain get/set (bind them with any
//        configuration binder); WithDefault / Validate are kept as the no-ops they are in Go.
// =====================================================================================================================
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Oxia.Common.Constant;
using Oxia.Common.Logging;

namespace Oxia.Common.Security;

/// <summary>Go: <c>tls.ClientAuthType</c> (only the values the options can produce).</summary>
internal enum ClientAuthType
{
    NoClientCert,
    VerifyClientCertIfGiven,
    RequireAndVerifyClientCert,
}

/// <summary>What Go's <c>makeCommonConfig</c> puts into the shared part of the <c>tls.Config</c>.</summary>
internal sealed record CommonTlsConfig(
    ushort MinVersion,
    ushort MaxVersion,
    string ServerName,
    bool InsecureSkipVerify,
    ushort[]? CipherSuites,
    SslProtocols Protocols);

/// <summary>The server configuration, with the pieces Go exposes on <c>tls.Config</c> (ClientAuth, ClientCAs) visible to tests.</summary>
internal sealed record ServerTlsConfig(
    SslServerAuthenticationOptions Options,
    ClientAuthType ClientAuth,
    X509Certificate2Collection? ClientCAs);

public sealed class TlsOptions
{
    /// <summary>tls.VersionTLS10.</summary>
    public const ushort VersionTls10 = 0x0301;
    /// <summary>tls.VersionTLS11.</summary>
    public const ushort VersionTls11 = 0x0302;
    /// <summary>tls.VersionTLS12.</summary>
    public const ushort VersionTls12 = 0x0303;
    /// <summary>tls.VersionTLS13.</summary>
    public const ushort VersionTls13 = 0x0304;

    public static readonly OxiaException ErrInvalidTlsCertFile = new("tls cert file path can not be empty");
    public static readonly OxiaException ErrInvalidTlsKeyFile = new("tls key file path can not be empty");

    /// <summary>Enable TLS.  When unset, TLS is enabled if <see cref="CertFile"/> is set.</summary>
    public bool? Enabled { get; set; }

    /// <summary>Path to the (server or client) certificate file, PEM.</summary>
    public string CertFile { get; set; } = string.Empty;

    /// <summary>Path to the private key file, PEM.</summary>
    public string KeyFile { get; set; } = string.Empty;

    /// <summary>Supported cipher suites (IANA codes), default: the platform's.</summary>
    public ushort[]? CipherSuites { get; set; }

    /// <summary>Minimum TLS version (0 = TLS 1.2).</summary>
    public ushort MinVersion { get; set; }

    /// <summary>Maximum TLS version (0 = the latest).</summary>
    public ushort MaxVersion { get; set; }

    /// <summary>Path to the CA certificate bundle.  When empty, certificate verification uses the system roots.</summary>
    public string TrustedCaFile { get; set; } = string.Empty;

    /// <summary>Skip verifying the certificate chain and host name.</summary>
    public bool InsecureSkipVerify { get; set; }

    /// <summary>Expected server name (for SNI and host name verification) when connecting to a server.</summary>
    public string ServerName { get; set; } = string.Empty;

    /// <summary>Whether the server requires clients to authenticate with a certificate.</summary>
    public bool ClientAuth { get; set; }

    public void WithDefault()
    {
    }

    public void Validate()
    {
    }

    public bool IsEnabled() => Enabled is null ? CertFile.Length > 0 : Enabled.Value;

    private void ValidateKeyPair()
    {
        if (CertFile.Length == 0) throw OxiaErrors.ToThrowable(ErrInvalidTlsCertFile);
        if (KeyFile.Length == 0) throw OxiaErrors.ToThrowable(ErrInvalidTlsKeyFile);

        using var _ = LoadKeyPair();
    }

    /// <summary>Go: <c>libtls.LoadX509KeyPair(CertFile, KeyFile)</c>; the caller owns the certificate.</summary>
    internal X509Certificate2 LoadKeyPair()
    {
        using var pem = X509Certificate2.CreateFromPemFile(CertFile, KeyFile);
        // A key created from PEM is ephemeral, which SChannel cannot use for TLS authentication: re-import it as PKCS#12
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    }

    internal CommonTlsConfig MakeCommonConfig()
    {
        var minVersion = VersionTls12;
        if (MinVersion != 0)
        {
            minVersion = MinVersion;
        }

        if (minVersion < VersionTls12)
        {
            throw new OxiaException($"minimum TLS version 0x{minVersion:x4} is below TLS 1.2 (0x{VersionTls12:x4})");
        }

        if (MaxVersion != 0 && MaxVersion < minVersion)
        {
            throw new OxiaException($"maximum TLS version 0x{MaxVersion:x4} is below minimum 0x{minVersion:x4}");
        }

        if (minVersion > VersionTls13)
        {
            throw new OxiaException($"minimum TLS version 0x{minVersion:x4} is above TLS 1.3 (0x{VersionTls13:x4})");
        }

        var protocols = SslProtocols.None;
        if (minVersion <= VersionTls12 && (MaxVersion == 0 || MaxVersion >= VersionTls12)) protocols |= SslProtocols.Tls12;
        if (MaxVersion == 0 || MaxVersion >= VersionTls13) protocols |= SslProtocols.Tls13;

        return new CommonTlsConfig(
            minVersion,
            MaxVersion,
            ServerName,
            InsecureSkipVerify,
            CipherSuites is { Length: > 0 } ? CipherSuites : null,
            protocols);
    }

    /// <summary>Go: <c>trustedCertPool()</c> -- every CERTIFICATE block of the CA file.</summary>
    internal X509Certificate2Collection TrustedCertPool()
    {
        var pool = new X509Certificate2Collection();
        var pem = File.ReadAllText(TrustedCaFile);

        var certCount = 0;
        var rest = pem.AsSpan();
        while (PemEncoding.TryFind(rest, out var fields))
        {
            var label = rest[fields.Label];
            var base64 = rest[fields.Base64Data];
            rest = rest[fields.Location.End..];
            if (!label.SequenceEqual("CERTIFICATE"))
            {
                continue;
            }

            byte[] der;
            try
            {
                der = Convert.FromBase64String(base64.ToString());
            }
            catch (FormatException)
            {
                continue; // pem.Decode skips a block it cannot decode
            }

            try
            {
                pool.Add(X509CertificateLoader.LoadCertificate(der));
            }
            catch (CryptographicException e)
            {
                throw new OxiaException($"failed to parse certificate in {TrustedCaFile}: {e.Message}", e);
            }

            certCount++;
        }

        if (certCount == 0)
        {
            throw new OxiaException($"no valid certificates found in {TrustedCaFile}");
        }

        return pool;
    }

    /// <summary>
    /// Go: <c>TryIntoClientTLSConf</c> -- null when TLS is not enabled; throws when the options are invalid.
    /// </summary>
    public SslClientAuthenticationOptions? TryIntoClientTlsConf()
    {
        if (!IsEnabled())
        {
            return null;
        }

        var common = MakeCommonConfig();
        var options = new SslClientAuthenticationOptions
        {
            EnabledSslProtocols = common.Protocols,
            TargetHost = ServerName.Length > 0 ? ServerName : null,
            CipherSuitesPolicy = ToPolicy(common.CipherSuites),
        };

        X509Certificate2Collection? rootCas = null;
        if (TrustedCaFile.Length > 0)
        {
            rootCas = TrustedCertPool();
        }

        if (InsecureSkipVerify)
        {
            options.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        }
        else if (rootCas is not null || options.TargetHost is not null)
        {
            // SocketsHttpHandler verifies the host name against the host it dials, ignoring TargetHost: verify the configured server
            // name (Go's tls.Config.ServerName) here, with the chain checked against the configured roots (or the system's)
            var serverName = options.TargetHost;
            options.RemoteCertificateValidationCallback = (_, certificate, chain, errors) =>
                ValidateServerCertificate(certificate, chain, errors, rootCas, serverName);
        }

        // The client certificate is optional: only a server that requires client authentication asks for one.
        if (CertFile.Length == 0 && KeyFile.Length == 0)
        {
            return options;
        }

        ValidateKeyPair();
        options.LocalCertificateSelectionCallback = (_, _, _, _, _) => LoadKeyPair();
        return options;
    }

    /// <summary>Go: <c>TryIntoServerTLSConf</c> -- null when TLS is not enabled; throws when the options are invalid.</summary>
    public SslServerAuthenticationOptions? TryIntoServerTlsConf(ILogger? logger = null) => BuildServerConfig(logger)?.Options;

    internal ServerTlsConfig? BuildServerConfig(ILogger? logger = null)
    {
        if (!IsEnabled())
        {
            return null;
        }

        ValidateKeyPair();
        var common = MakeCommonConfig();

        // auth type upgrading
        var authType = ClientAuthType.NoClientCert;
        if (TrustedCaFile.Length > 0)
        {
            authType = ClientAuthType.VerifyClientCertIfGiven;
        }

        if (ClientAuth)
        {
            authType = ClientAuthType.RequireAndVerifyClientCert;
        }

        X509Certificate2Collection? clientCas = null;
        if (TrustedCaFile.Length > 0)
        {
            clientCas = TrustedCertPool();
        }
        else if (ClientAuth)
        {
            (logger ?? OxiaLog.Create("tls")).LogWarning(
                "TLS client authentication is using system roots because trustedCaFile is not set. hint={Hint}",
                "Set trustedCaFile to explicitly configure trusted client CAs");
        }

        var options = new SslServerAuthenticationOptions
        {
            EnabledSslProtocols = common.Protocols,
            CipherSuitesPolicy = ToPolicy(common.CipherSuites),
            ServerCertificateSelectionCallback = (_, _) => LoadKeyPair(),
            ClientCertificateRequired = authType != ClientAuthType.NoClientCert,
        };

        if (clientCas is not null && authType != ClientAuthType.NoClientCert)
        {
            var require = authType == ClientAuthType.RequireAndVerifyClientCert;
            options.RemoteCertificateValidationCallback = (_, certificate, chain, errors) =>
            {
                if (certificate is null)
                {
                    // VerifyClientCertIfGiven accepts a handshake without a certificate
                    return !require;
                }

                return ChainsToRoots(certificate, chain, clientCas, serverAuth: false);
            };
        }

        return new ServerTlsConfig(options, authType, clientCas);
    }

    /// <summary>x509 verification of a server certificate: the chain against <paramref name="roots"/> (the system roots when null) and the host name.</summary>
    internal static bool ValidateServerCertificate(X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors,
        X509Certificate2Collection? roots, string? serverName)
    {
        if (certificate is null || (errors & SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
        {
            return false;
        }

        if (roots is null)
        {
            if ((errors & SslPolicyErrors.RemoteCertificateChainErrors) != 0) return false;
        }
        else if (!ChainsToRoots(certificate, chain, roots, serverAuth: true))
        {
            return false;
        }

        if (serverName is null)
        {
            return (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0;
        }

        using var leaf = new X509Certificate2(certificate);
        return leaf.MatchesHostname(serverName);
    }

    private static CipherSuitesPolicy? ToPolicy(ushort[]? cipherSuites)
    {
        if (cipherSuites is null)
        {
            return null;
        }

        if (OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "TlsOptions.CipherSuites cannot be applied on Windows: .NET does not support CipherSuitesPolicy there");
        }

        return new CipherSuitesPolicy(cipherSuites.Select(static c => (TlsCipherSuite)c));
    }

    /// <summary>x509 verification of <paramref name="certificate"/> against <paramref name="roots"/> only (no revocation checks).</summary>
    internal static bool ChainsToRoots(X509Certificate? certificate, X509Chain? presented, X509Certificate2Collection roots, bool serverAuth)
    {
        if (certificate is null)
        {
            return false;
        }

        using var leaf = new X509Certificate2(certificate);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(roots);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(new Oid(serverAuth ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"));
        if (presented is not null)
        {
            // intermediates sent by the peer
            foreach (var element in presented.ChainElements)
            {
                chain.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        return chain.Build(leaf);
    }
}
