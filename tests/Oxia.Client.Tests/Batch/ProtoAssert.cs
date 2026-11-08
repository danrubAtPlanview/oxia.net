// =====================================================================================================================
// PORT NOTE (test helper, no Go counterpart)
//   Upstream : testify's assert.Equal on protobuf structs (reflect.DeepEqual) in oxia/internal/batch/*_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Deep, field by field comparison of the hand-written proto message classes (public fields and properties,
//              nested messages, lists, arrays, ReadOnlyMemory<byte>, nullables).  The members compared are those declared by
//              the EXPECTED object's type, so an expected PutRequest matches an actual PutCall (a call IS its own request):
//              the call-only fields (CallContext, Callback, ...) are not part of the wire message and are not compared.
// =====================================================================================================================
using System.Collections;
using System.Reflection;
using Xunit;

namespace Oxia.Client.Tests.Batch;

internal static class ProtoAssert
{
    public static void Equal(object? expected, object? actual, string path = "$")
    {
        string? diff = Diff(expected, actual, path);
        Assert.True(diff is null, diff);
    }

    private static string? Diff(object? expected, object? actual, string path)
    {
        if (expected is null || actual is null)
        {
            return expected is null && actual is null ? null : $"{path}: expected {Show(expected)}, actual {Show(actual)}";
        }

        var type = expected.GetType();
        if (expected is ReadOnlyMemory<byte> memory)
        {
            return actual is ReadOnlyMemory<byte> other && memory.Span.SequenceEqual(other.Span)
                ? null
                : $"{path}: expected {Show(expected)}, actual {Show(actual)}";
        }

        if (type.IsPrimitive || type.IsEnum || expected is string || expected is decimal)
        {
            return expected.Equals(actual) ? null : $"{path}: expected {Show(expected)}, actual {Show(actual)}";
        }

        if (expected is IEnumerable expectedItems && actual is IEnumerable actualItems)
        {
            var e = expectedItems.Cast<object?>().ToList();
            var a = actualItems.Cast<object?>().ToList();
            if (e.Count != a.Count) return $"{path}: expected {e.Count} items, actual {a.Count}";
            for (int i = 0; i < e.Count; i++)
            {
                var d = Diff(e[i], a[i], $"{path}[{i}]");
                if (d is not null) return d;
            }
            return null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            return expected.Equals(actual) ? null : $"{path}: expected {Show(expected)}, actual {Show(actual)}";
        }

        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            var d = Diff(field.GetValue(expected), field.GetValue(actual), $"{path}.{field.Name}");
            if (d is not null) return d;
        }
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length != 0 || !property.CanRead) continue;
            var d = Diff(property.GetValue(expected), property.GetValue(actual), $"{path}.{property.Name}");
            if (d is not null) return d;
        }
        return null;
    }

    private static string Show(object? value) => value switch
    {
        null => "null",
        ReadOnlyMemory<byte> memory => "[" + string.Join(",", memory.ToArray()) + "]",
        _ => value.ToString() ?? value.GetType().Name,
    };
}
