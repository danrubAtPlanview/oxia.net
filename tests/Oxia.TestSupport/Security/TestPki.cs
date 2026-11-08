// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : tests/security/certs/{generate_command.txt,openssl.conf} + the committed ca/client/peer .crt/.key files that
//              tests/security/tls/tls_encryption_test.go loads (getPeerTLSOption / getClientTLSOption).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Go tests ship a pre-generated CA + `peer` (server and peer: serverAuth+clientAuth) + `client` certificate set.
//              This builds the same set at test time with System.Security.Cryptography.X509Certificates.CertificateRequest:
//                  CA      CN=oxia-ca      self signed, CA:true
//                  peer    CN=oxia-peer    signed by the CA
//                  client  CN=oxia-client  signed by the CA
//              Both leaf certificates carry what openssl.conf puts into them: keyUsage (critical) digitalSignature+keyEncipherment,
//              extendedKeyUsage serverAuth+clientAuth, subjectAltName DNS:localhost, IP:127.0.0.1, IP:192.168.1.2; RSA 2048 keys.
//              `WriteTo(dir)` writes the six PEM files (ca.crt, peer.crt/key, client.crt/key; keys are PKCS#8 "PRIVATE KEY") so
//              they can be referenced by Oxia.Common.Security.TlsOptions on the client side, and `PeerFiles` hands the same
//              content to the server side (copied into the Docker container's tmpfs by the cluster fixture).
//   Divergences: generated per test run instead of committed (no key material in the repository); the validity starts one day in
//              the past to tolerate clock drift between the host and the Docker VM; the CA private key is discarded after the
//              leaves are signed (Go keeps ca.key/ca.srl in the repository, nothing reads them).
// =====================================================================================================================
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Oxia.TestSupport.Security;

/// <summary>One certificate and its private key, PEM encoded.</summary>
public sealed record PemIdentity(string CertificatePem, string PrivateKeyPem);

/// <summary>A throw-away CA with the <c>peer</c> and <c>client</c> certificates of the Go <c>tests/security/certs</c> directory.</summary>
public sealed class TestPki : IDisposable
{
    public const string CaFileName = "ca.crt";
    public const string PeerCertFileName = "peer.crt";
    public const string PeerKeyFileName = "peer.key";
    public const string ClientCertFileName = "client.crt";
    public const string ClientKeyFileName = "client.key";

    private readonly X509Certificate2 _ca;

    private TestPki(X509Certificate2 ca, PemIdentity peer, PemIdentity client)
    {
        _ca = ca;
        CaCertificatePem = ca.ExportCertificatePem();
        Peer = peer;
        Client = client;
    }

    /// <summary>Go: <c>certs/ca.crt</c>.</summary>
    public string CaCertificatePem { get; }

    /// <summary>Go: <c>certs/peer.crt</c> + <c>peer.key</c> (the server, internal and replication certificate).</summary>
    public PemIdentity Peer { get; }

    /// <summary>Go: <c>certs/client.crt</c> + <c>client.key</c>.</summary>
    public PemIdentity Client { get; }

    /// <summary>Builds the CA and the two leaf certificates.</summary>
    public static TestPki Create()
    {
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = DateTimeOffset.UtcNow.AddDays(3650);

        // openssl req -x509 -new -nodes -key ca.key -subj "/CN=oxia-ca" -days 3650
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=oxia-ca", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature, critical: true));
        caRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caRequest.PublicKey, critical: false));
        var ca = caRequest.CreateSelfSigned(notBefore, notAfter);

        var peer = CreateLeaf(ca, "CN=oxia-peer", notBefore, notAfter);
        var client = CreateLeaf(ca, "CN=oxia-client", notBefore, notAfter);
        return new TestPki(ca, peer, client);
    }

    /// <summary>openssl req -new ... + openssl x509 -req -extensions oxia_req -extfile openssl.conf.</summary>
    private static PemIdentity CreateLeaf(X509Certificate2 ca, string subject, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: false, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1", "serverAuth"), new Oid("1.3.6.1.5.5.7.3.2", "clientAuth")], critical: false));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, critical: false));

        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Parse("127.0.0.1"));
        san.AddIpAddress(IPAddress.Parse("192.168.1.2"));
        request.CertificateExtensions.Add(san.Build());

        var serial = new byte[16];
        RandomNumberGenerator.Fill(serial);
        serial[0] &= 0x7F; // positive
        using var certificate = request.Create(ca, notBefore, notAfter, serial);
        return new PemIdentity(certificate.ExportCertificatePem(), PemEncodingOf("PRIVATE KEY", key.ExportPkcs8PrivateKey()));
    }

    private static string PemEncodingOf(string label, byte[] der) => new(PemEncoding.Write(label, der));

    /// <summary>
    /// The files of the Go <c>certs</c> directory by name (ca.crt, peer.crt, peer.key, client.crt, client.key), ready to be copied
    /// into a server's directory.
    /// </summary>
    public IReadOnlyDictionary<string, string> AllFiles() => new Dictionary<string, string>
    {
        [CaFileName] = CaCertificatePem,
        [PeerCertFileName] = Peer.CertificatePem,
        [PeerKeyFileName] = Peer.PrivateKeyPem,
        [ClientCertFileName] = Client.CertificatePem,
        [ClientKeyFileName] = Client.PrivateKeyPem,
    };

    /// <summary>The server-side files only (no client key travels to the container): the CA and the <c>peer</c> pair.</summary>
    public IReadOnlyDictionary<string, string> PeerFiles() => new Dictionary<string, string>
    {
        [CaFileName] = CaCertificatePem,
        [PeerCertFileName] = Peer.CertificatePem,
        [PeerKeyFileName] = Peer.PrivateKeyPem,
    };

    /// <summary>Writes <see cref="AllFiles"/> into <paramref name="directory"/> (created if needed); returns the directory.</summary>
    public string WriteTo(string directory)
    {
        Directory.CreateDirectory(directory);
        foreach (var (name, content) in AllFiles())
        {
            File.WriteAllText(Path.Combine(directory, name), content);
        }

        return directory;
    }

    /// <summary>
    /// Go: <c>getClientTLSOption()</c> -- the client certificate and the CA, as files in <paramref name="certDirectory"/>
    /// (which must hold the output of <see cref="WriteTo"/>).
    /// </summary>
    public static Oxia.Common.Security.TlsOptions ClientTlsOptions(string certDirectory) => new()
    {
        CertFile = Path.Combine(certDirectory, ClientCertFileName),
        KeyFile = Path.Combine(certDirectory, ClientKeyFileName),
        TrustedCaFile = Path.Combine(certDirectory, CaFileName),
    };

    /// <summary>Go: <c>getPeerTLSOption()</c> -- the peer certificate and the CA, as files in <paramref name="certDirectory"/>.</summary>
    public static Oxia.Common.Security.TlsOptions PeerTlsOptions(string certDirectory) => new()
    {
        CertFile = Path.Combine(certDirectory, PeerCertFileName),
        KeyFile = Path.Combine(certDirectory, PeerKeyFileName),
        TrustedCaFile = Path.Combine(certDirectory, CaFileName),
    };

    /// <summary>
    /// TLS options for the fixture's own raw gRPC probes (readiness, leader lookup): trust the CA, verify the host name, no client
    /// certificate.  Deliberately independent of <c>Oxia.Common.Security.TlsOptions</c>, which is part of what the tests exercise.
    /// </summary>
    public SslClientAuthenticationOptions RawClientSslOptions()
    {
        var root = X509CertificateLoader.LoadCertificate(_ca.RawData);
        return new SslClientAuthenticationOptions
        {
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            RemoteCertificateValidationCallback = (_, certificate, _, _) =>
            {
                if (certificate is null) return false;
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(root);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                using var leaf = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
                return chain.Build(leaf);
            },
        };
    }

    public void Dispose() => _ca.Dispose();
}
