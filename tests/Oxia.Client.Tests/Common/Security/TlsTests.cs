// =====================================================================================================================
// PORT NOTE
//   Upstream : common/security/tls_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : TLS option handling: the CA pool parsing, version / cipher-suite validation, optional client certificate on the
//              client, mandatory key pair on the server, and the client-auth level the server derives from the options.
//   Go tests: 16 functions (TestTryIntoClientTLSConf_ClientCertificate 4 subtests, TestTryIntoServerTLSConf_ClientAuthTrust 5 subtests),
//              ported: 16, skipped: 0.
//   Divergences:
//      * `libtls.Config` -> SslClient/ServerAuthenticationOptions.  Fields Go asserts on that .NET options do not carry are read from
//        the internal records TlsOptions builds first (CommonTlsConfig.MinVersion / MaxVersion / CipherSuites,
//        ServerTlsConfig.ClientAuth / ClientCAs); `conf.GetClientCertificate != nil` is `LocalCertificateSelectionCallback != null`.
//      * x509.CertPool.Subjects() -> X509Certificate2Collection.Count.
//      * `assert.ErrorIs(err, os.ErrNotExist)` -> FileNotFoundException / DirectoryNotFoundException; `assert.ErrorIs(err, ErrInvalid...)` ->
//        OxiaErrors.Is.
//      * The slog capture of TestTryIntoServerTLSConf_ClientAuthTrust is a capturing ILogger passed to TryIntoServerTlsConf.
//      * Cipher suites cannot be applied on Windows (no CipherSuitesPolicy): the conf-level assertions of
//        TestTLSOptionsMakeCommonConfigCipherSuites run everywhere, the SslOptions-level check is an extra test that is skipped there.
//      * The self-signed test certificates are generated with CertificateRequest (ECDSA P-256), PEM-exported like the Go helper.
// =====================================================================================================================
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Oxia.Common.Constant;
using Oxia.Common.Security;
using Xunit;

namespace Oxia.Client.Tests.Common;

internal sealed class TempDir : IDisposable
{
    public TempDir()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "oxia-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public string Write(string name, string content)
    {
        var path = System.IO.Path.Combine(Path, name);
        File.WriteAllText(path, content);
        return path;
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, true);
        }
        catch (IOException)
        {
            // best effort
        }
    }
}

internal sealed class CapturingLogger : ILogger
{
    public List<(LogLevel Level, string Message)> Entries { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => Entries.Add((logLevel, formatter(state, exception)));
}

public class TlsTests
{
    internal static (string CertPem, string KeyPem) GenerateSelfSignedCert(string cn, params string[] dnsNames)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature, true));
        if (dnsNames.Length > 0)
        {
            var san = new SubjectAlternativeNameBuilder();
            foreach (var name in dnsNames) san.AddDnsName(name);
            request.CertificateExtensions.Add(san.Build());
        }

        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
        // pem.EncodeToMemory output ends with a newline (so that PEM blocks can be concatenated)
        return (cert.ExportCertificatePem() + "\n", key.ExportECPrivateKeyPem() + "\n");
    }

    private static bool IsNotExist(Exception? e) => e is FileNotFoundException or DirectoryNotFoundException;

    [Fact]
    public void TestTrustedCertPool_MultipleCerts()
    {
        using var dir = new TempDir();

        var (cert1Pem, _) = GenerateSelfSignedCert("CA1");
        var (cert2Pem, _) = GenerateSelfSignedCert("CA2");

        // Bundle both certs into one PEM file
        var bundlePath = dir.Write("ca-bundle.pem", cert1Pem + cert2Pem);

        var opts = new TlsOptions { TrustedCaFile = bundlePath };
        var pool = opts.TrustedCertPool();

        // The pool should contain both certificates
        Assert.Equal(2, pool.Count);
    }

    [Fact]
    public void TestTrustedCertPool_SingleCert()
    {
        using var dir = new TempDir();

        var (certPem, _) = GenerateSelfSignedCert("SingleCA");
        var caPath = dir.Write("ca.pem", certPem);

        var opts = new TlsOptions { TrustedCaFile = caPath };
        var pool = opts.TrustedCertPool();

        Assert.Single(pool);
    }

    [Fact]
    public void TestTrustedCertPool_NoCerts()
    {
        using var dir = new TempDir();

        // Write a PEM file with only a private key block (no CERTIFICATE)
        var keyBlock = PemEncoding.WriteString("EC PRIVATE KEY", "fake"u8) + "\n";
        var keyPath = dir.Write("no-certs.pem", keyBlock);

        var opts = new TlsOptions { TrustedCaFile = keyPath };
        var err = Assert.Throws<OxiaException>(() => opts.TrustedCertPool());
        Assert.Contains("no valid certificates found", err.Message);
    }

    [Fact]
    public void TestTrustedCertPool_InvalidCert()
    {
        using var dir = new TempDir();

        // Write a PEM file with an invalid CERTIFICATE block
        var badCert = PemEncoding.WriteString("CERTIFICATE", "not a valid cert"u8) + "\n";
        var badPath = dir.Write("bad-cert.pem", badCert);

        var opts = new TlsOptions { TrustedCaFile = badPath };
        var err = Assert.Throws<OxiaException>(() => opts.TrustedCertPool());
        Assert.Contains("failed to parse certificate", err.Message);
    }

    [Fact]
    public void TestTrustedCertPool_FileNotFound()
    {
        var opts = new TlsOptions { TrustedCaFile = "/nonexistent/path/ca.pem" };
        Assert.ThrowsAny<Exception>(() => opts.TrustedCertPool());
    }

    [Fact]
    public void TestMakeCommonConfig_MinVersionBelowTLS12()
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("test");
        var certPath = dir.Write("cert.pem", certPem);
        var keyPath = dir.Write("key.pem", keyPem);

        var opts = new TlsOptions
        {
            CertFile = certPath,
            KeyFile = keyPath,
            MinVersion = TlsOptions.VersionTls10,
        };
        var err = Assert.Throws<OxiaException>(() => opts.MakeCommonConfig());
        Assert.Contains("below TLS 1.2", err.Message);
    }

    [Fact]
    public void TestMakeCommonConfig_MaxVersionBelowMin()
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("test");
        var certPath = dir.Write("cert.pem", certPem);
        var keyPath = dir.Write("key.pem", keyPem);

        var opts = new TlsOptions
        {
            CertFile = certPath,
            KeyFile = keyPath,
            MinVersion = TlsOptions.VersionTls13,
            MaxVersion = TlsOptions.VersionTls12,
        };
        var err = Assert.Throws<OxiaException>(() => opts.MakeCommonConfig());
        Assert.Contains("below minimum", err.Message);
    }

    [Fact]
    public void TestMakeCommonConfig_ValidVersions()
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("test");
        var certPath = dir.Write("cert.pem", certPem);
        var keyPath = dir.Write("key.pem", keyPem);

        var opts = new TlsOptions
        {
            CertFile = certPath,
            KeyFile = keyPath,
            MinVersion = TlsOptions.VersionTls12,
            MaxVersion = TlsOptions.VersionTls13,
        };
        var conf = opts.MakeCommonConfig();
        Assert.Equal(TlsOptions.VersionTls12, conf.MinVersion);
        Assert.Equal(TlsOptions.VersionTls13, conf.MaxVersion);
    }

    [Fact]
    public void TestTLSOptionsMakeCommonConfigCipherSuites()
    {
        ushort[] suites = [(ushort)TlsCipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256];
        var opts = new TlsOptions { CipherSuites = suites };
        var conf = opts.MakeCommonConfig();
        Assert.Equal(suites, conf.CipherSuites);

        conf = new TlsOptions().MakeCommonConfig();
        Assert.Null(conf.CipherSuites);
    }

    [Fact]
    public void TestTLSOptionsTryIntoClientTLSConfInvalidVersion()
    {
        var opts = new TlsOptions { Enabled = true, MinVersion = TlsOptions.VersionTls11 };
        var err = Assert.Throws<OxiaException>(() => opts.TryIntoClientTlsConf());
        Assert.Contains("below TLS 1.2", err.Message);
    }

    [Fact]
    public void TestTLSOptionsTryIntoClientTLSConfMissingTrustedCaFile()
    {
        using var dir = new TempDir();
        var opts = new TlsOptions { Enabled = true, TrustedCaFile = Path.Combine(dir.Path, "missing.pem") };
        var err = Assert.ThrowsAny<Exception>(() => opts.TryIntoClientTlsConf());
        Assert.True(IsNotExist(err), err.ToString()); // assert.ErrorIs(err, os.ErrNotExist)
    }

    public static TheoryData<string> ClientCertificateCases => ["no client certificate", "client certificate", "certificate without key", "key without certificate"];

    [Theory]
    [MemberData(nameof(ClientCertificateCases))]
    public void TestTryIntoClientTLSConf_ClientCertificate(string name)
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("client");
        var certPath = dir.Write("cert.pem", certPem);
        var keyPath = dir.Write("key.pem", keyPem);

        var (certFile, keyFile, wantErr, wantClientCert) = name switch
        {
            "no client certificate" => ("", "", (Exception?)null, false),
            "client certificate" => (certPath, keyPath, null, true),
            "certificate without key" => (certPath, "", TlsOptions.ErrInvalidTlsKeyFile, false),
            "key without certificate" => ("", keyPath, TlsOptions.ErrInvalidTlsCertFile, false),
            _ => throw new ArgumentException(name),
        };

        var opts = new TlsOptions { Enabled = true, CertFile = certFile, KeyFile = keyFile };
        if (wantErr is not null)
        {
            var err = Assert.ThrowsAny<Exception>(() => opts.TryIntoClientTlsConf());
            Assert.True(OxiaErrors.Is(err, wantErr), name);
            return;
        }

        var conf = opts.TryIntoClientTlsConf();
        Assert.NotNull(conf);
        Assert.Equal(wantClientCert, conf.LocalCertificateSelectionCallback is not null);
    }

    [Fact]
    public void TestTryIntoServerTLSConf_RequiresCertificate()
    {
        var opts = new TlsOptions { Enabled = true };
        var err = Assert.ThrowsAny<Exception>(() => opts.TryIntoServerTlsConf());
        Assert.True(OxiaErrors.Is(err, TlsOptions.ErrInvalidTlsCertFile));
    }

    [Fact]
    public void TestTLSOptionsTryIntoServerTLSConfInvalidVersion()
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("server");
        var opts = new TlsOptions
        {
            CertFile = dir.Write("cert.pem", certPem),
            KeyFile = dir.Write("key.pem", keyPem),
            MinVersion = TlsOptions.VersionTls13,
            MaxVersion = TlsOptions.VersionTls12,
        };
        var err = Assert.Throws<OxiaException>(() => opts.TryIntoServerTlsConf());
        Assert.Contains("below minimum", err.Message);
    }

    [Fact]
    public void TestTLSOptionsTryIntoServerTLSConfMissingTrustedCaFile()
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("server");
        var opts = new TlsOptions
        {
            CertFile = dir.Write("cert.pem", certPem),
            KeyFile = dir.Write("key.pem", keyPem),
            TrustedCaFile = Path.Combine(dir.Path, "missing.pem"),
        };
        var err = Assert.ThrowsAny<Exception>(() => opts.TryIntoServerTlsConf());
        Assert.True(IsNotExist(err), err.ToString()); // assert.ErrorIs(err, os.ErrNotExist)
    }

    public static TheoryData<string> ClientAuthTrustCases =>
    [
        "required client authentication with system roots",
        "required client authentication with explicit CA",
        "no client authentication",
        "optional client authentication with explicit CA",
        "TLS disabled",
    ];

    [Theory]
    [MemberData(nameof(ClientAuthTrustCases))]
    public void TestTryIntoServerTLSConf_ClientAuthTrust(string name)
    {
        using var dir = new TempDir();
        var (certPem, keyPem) = GenerateSelfSignedCert("server");
        var certPath = dir.Write("cert.pem", certPem);
        var keyPath = dir.Write("key.pem", keyPem);
        var (caPem, _) = GenerateSelfSignedCert("ClientCA");
        var caPath = dir.Write("ca.pem", caPem);

        var (enabled, clientAuth, trustedCaFile, wantClientAuth, wantWarning) = name switch
        {
            "required client authentication with system roots" => ((bool?)null, true, "", ClientAuthType.RequireAndVerifyClientCert, true),
            "required client authentication with explicit CA" => (null, true, caPath, ClientAuthType.RequireAndVerifyClientCert, false),
            "no client authentication" => (null, false, "", ClientAuthType.NoClientCert, false),
            "optional client authentication with explicit CA" => (null, false, caPath, ClientAuthType.VerifyClientCertIfGiven, false),
            "TLS disabled" => (false, true, "", ClientAuthType.NoClientCert, false),
            _ => throw new ArgumentException(name),
        };

        var logger = new CapturingLogger();
        var opts = new TlsOptions
        {
            Enabled = enabled,
            CertFile = certPath,
            KeyFile = keyPath,
            TrustedCaFile = trustedCaFile,
            ClientAuth = clientAuth,
        };
        var conf = opts.BuildServerConfig(logger);
        if (wantWarning)
        {
            var warning = Assert.Single(logger.Entries);
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.Contains("using system roots", warning.Message);
            Assert.Contains("trustedCaFile", warning.Message);
        }
        else
        {
            Assert.Empty(logger.Entries);
        }

        if (!opts.IsEnabled())
        {
            Assert.Null(conf);
            Assert.Null(opts.TryIntoServerTlsConf(logger));
            return;
        }

        Assert.NotNull(conf);
        Assert.Equal(wantClientAuth, conf.ClientAuth);
        Assert.Equal(wantClientAuth != ClientAuthType.NoClientCert, conf.Options.ClientCertificateRequired);
        if (trustedCaFile.Length == 0)
        {
            Assert.Null(conf.ClientCAs);
        }
        else
        {
            Assert.NotNull(conf.ClientCAs);
            Assert.Single(conf.ClientCAs);
        }
    }
}
