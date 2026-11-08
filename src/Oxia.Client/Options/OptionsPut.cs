// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_put.go   (putOptions, PutOption, newPutOptions, ExpectedRecordNotExists, Ephemeral,
//              SequenceKeysDeltas, secondaryIdxOption, SecondaryIndex)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the Put operation, applied in order (last wins) and then validated.
//   Divergences:
//      * Struct options over the shared OptionPayload (see OptionsBase.cs).  `ExpectedRecordNotExists()` is
//        ExpectedVersionId(-1) typed as a PutOption only (Go returns the same `expectedVersionId` value typed PutOption).
//      * `newPutOptions` returns `(*putOptions, error)`; here TryCreate returns the error (null on success) and the
//        resolved options in `result` (default on error, like Go's nil).  The error messages and the validation order are
//        byte-identical: secondary index names first (the first one containing '/' wins), then the sequence-keys checks.
//        Go's `%q` is reproduced by <see cref="GoQuote"/>.
//      * The list of secondary indexes is only allocated when a SecondaryIndex option is present.
//      * SequenceKeysDeltas keeps `params ulong[]` (the array is stored as is, like Go's slice: it is the one allocation of
//        that option, and it is the very array that ends up in the PutRequest).
//      * SecondaryIndex(null, ...) / SecondaryIndex(..., null) throw ArgumentNullException.
// =====================================================================================================================
using System.Globalization;
using System.Text;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;

namespace Oxia.Client;

/// <summary>Represents an option for the Put operation (Go: PutOption).</summary>
public readonly struct PutOption
{
    internal readonly OptionPayload P;

    internal PutOption(in OptionPayload p) => P = p;
}

/// <summary>Go: secondaryIdxOption.</summary>
internal readonly record struct SecondaryIdxOption(string IndexName, string SecondaryKey);

/// <summary>Go: putOptions + newPutOptions(opts) (validation errors returned, not thrown).</summary>
internal struct PutOptions
{
    public string? PartitionKey;
    public long? ExpectedVersion;
    public bool Ephemeral;
    public ulong[]? SequenceKeysDeltas;
    public List<SecondaryIdxOption>? SecondaryIndexes;
    public CallContext? CallContext;

    /// <summary>
    /// Applies the options in order and validates (index names without '/', sequence keys need PartitionKey and no
    /// ExpectedVersion and a first delta > 0).  Returns the Go error (OxiaErrors.Wrap(ClientErrors.InvalidOptions, "..."))
    /// or null.
    /// </summary>
    public static Exception? TryCreate(ReadOnlySpan<PutOption> options, out PutOptions result)
    {
        var o = new PutOptions();
        foreach (ref readonly var opt in options)
        {
            o.Apply(in opt.P);
        }

        if (o.SecondaryIndexes is { } indexes)
        {
            foreach (var secondaryIndex in indexes)
            {
                if (secondaryIndex.IndexName.Contains('/'))
                {
                    result = default;
                    return OxiaErrors.Wrap(ClientErrors.InvalidOptions,
                        "secondary index name " + GoQuote(secondaryIndex.IndexName) + " must not contain '/'");
                }
            }
        }

        if (o.SequenceKeysDeltas is { Length: > 0 } deltas)
        {
            if (o.PartitionKey is null)
            {
                result = default;
                return OxiaErrors.Wrap(ClientErrors.InvalidOptions, "usage of sequential keys requires PartitionKey() to be set");
            }

            if (o.ExpectedVersion is not null)
            {
                result = default;
                return OxiaErrors.Wrap(ClientErrors.InvalidOptions,
                    "usage of sequential keys does not allow to specify an ExpectedVersionId");
            }

            if (deltas[0] == 0)
            {
                result = default;
                return OxiaErrors.Wrap(ClientErrors.InvalidOptions, "first delta in sequence keys delta must always be > 0");
            }
        }

        result = o;
        return null;
    }

    private void Apply(in OptionPayload p)
    {
        switch (p.Kind)
        {
            case OptionKind.PartitionKey:        // partitionKeyOpt.applyPut
                PartitionKey = p.S1;
                break;
            case OptionKind.ExpectedVersionId:   // expectedVersionId.applyPut
                ExpectedVersion = p.Long;
                break;
            case OptionKind.Ephemeral:           // ephemeral.applyPut
                Ephemeral = true;
                break;
            case OptionKind.SequenceKeysDeltas:  // sequenceKeysDeltas.applyPut
                SequenceKeysDeltas = (ulong[]?)p.Obj;
                break;
            case OptionKind.SecondaryIndex:      // secondaryIdxOption.applyPut
                (SecondaryIndexes ??= new List<SecondaryIdxOption>(2)).Add(new SecondaryIdxOption(p.S1!, p.S2!));
                break;
            case OptionKind.CallContext:         // callContextOption.applyPut
                CallContext = (CallContext?)p.Obj;
                break;
        }
    }

    /// <summary>Go's <c>%q</c> verb (strconv.Quote): a double-quoted string with Go escapes, printable runes kept as they are.</summary>
    internal static string GoQuote(string s)
    {
        var sb = new StringBuilder(s.Length + 2);
        sb.Append('"');
        for (int i = 0; i < s.Length;)
        {
            if (!Rune.TryGetRuneAt(s, i, out var r))
            {
                // A lone surrogate is not valid UTF-8: Go prints the RuneError (U+FFFD) escaped
                sb.Append("\\ufffd");
                i++;
                continue;
            }

            i += r.Utf16SequenceLength;
            switch (r.Value)
            {
                case '"': sb.Append("\\\""); continue;
                case '\\': sb.Append("\\\\"); continue;
                case 7: sb.Append("\\a"); continue;
                case 8: sb.Append("\\b"); continue;
                case 12: sb.Append("\\f"); continue;
                case '\n': sb.Append("\\n"); continue;
                case '\r': sb.Append("\\r"); continue;
                case '\t': sb.Append("\\t"); continue;
                case 11: sb.Append("\\v"); continue;
            }

            if (IsPrint(r))
            {
                sb.Append(r.ToString());
            }
            else if (r.Value < ' ' || r.Value == 0x7f)
            {
                sb.Append("\\x").Append(r.Value.ToString("x2", CultureInfo.InvariantCulture));
            }
            else if (r.Value < 0x10000)
            {
                sb.Append("\\u").Append(r.Value.ToString("x4", CultureInfo.InvariantCulture));
            }
            else
            {
                sb.Append("\\U").Append(r.Value.ToString("x8", CultureInfo.InvariantCulture));
            }
        }

        sb.Append('"');
        return sb.ToString();
    }

    /// <summary>strconv.IsPrint: letters, marks, numbers, punctuation, symbols and the ASCII space.</summary>
    private static bool IsPrint(Rune r)
    {
        if (r.Value == ' ') return true;
        switch (Rune.GetUnicodeCategory(r))
        {
            case UnicodeCategory.UppercaseLetter:
            case UnicodeCategory.LowercaseLetter:
            case UnicodeCategory.TitlecaseLetter:
            case UnicodeCategory.ModifierLetter:
            case UnicodeCategory.OtherLetter:
            case UnicodeCategory.NonSpacingMark:
            case UnicodeCategory.SpacingCombiningMark:
            case UnicodeCategory.EnclosingMark:
            case UnicodeCategory.DecimalDigitNumber:
            case UnicodeCategory.LetterNumber:
            case UnicodeCategory.OtherNumber:
            case UnicodeCategory.ConnectorPunctuation:
            case UnicodeCategory.DashPunctuation:
            case UnicodeCategory.OpenPunctuation:
            case UnicodeCategory.ClosePunctuation:
            case UnicodeCategory.InitialQuotePunctuation:
            case UnicodeCategory.FinalQuotePunctuation:
            case UnicodeCategory.OtherPunctuation:
            case UnicodeCategory.MathSymbol:
            case UnicodeCategory.CurrencySymbol:
            case UnicodeCategory.ModifierSymbol:
            case UnicodeCategory.OtherSymbol:
                return true;
            default:
                return false;
        }
    }
}

public static partial class OxiaOptions
{
    /// <summary>Marks that the put operation should only be successful if the record does not exist yet.</summary>
    public static PutOption ExpectedRecordNotExists() =>
        new(new OptionPayload(OptionKind.ExpectedVersionId, RecordVersion.VersionIdNotExists));

    private static readonly PutOption EphemeralFlag = new(new OptionPayload(OptionKind.Ephemeral));

    /// <summary>
    /// Marks the record to be created as an ephemeral record.  Ephemeral records have their lifecycle tied to a particular
    /// client instance, and they are automatically deleted when the client instance is closed.  These records are also
    /// deleted if the client cannot communicate with the Oxia service for some extended amount of time, and the session
    /// between the client and the service "expires".  Application can control the session behavior by setting the session
    /// timeout appropriately with <see cref="WithSessionTimeout"/> option when creating the client instance.
    /// </summary>
    public static PutOption Ephemeral() => EphemeralFlag;

    /// <summary>
    /// Requests that the final record key be assigned by the server, based on the prefix record key and appending one or
    /// more sequences.  The sequence numbers will be atomically added based on the deltas.  Deltas must be >= 0 and the
    /// first one strictly > 0.  If the sequence already has more numbers than deltas, the missing deltas are 0.
    /// SequenceKeysDeltas also requires that a <see cref="PartitionKey"/> option is provided.
    /// </summary>
    public static PutOption SequenceKeysDeltas(params ulong[] delta) =>
        new(new OptionPayload(OptionKind.SequenceKeysDeltas, obj: delta));

    /// <summary>
    /// Lets users specify additional keys to index the record.  Index names must not contain '/' and can be used in
    /// <see cref="UseIndex"/> options.  Secondary keys are not required to be unique.  Multiple secondary indexes can be
    /// passed on the same record, even reusing multiple times the same indexName.
    /// </summary>
    public static PutOption SecondaryIndex(string indexName, string secondaryKey)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(secondaryKey);
        return new PutOption(new OptionPayload(OptionKind.SecondaryIndex, s1: indexName, s2: secondaryKey));
    }
}
