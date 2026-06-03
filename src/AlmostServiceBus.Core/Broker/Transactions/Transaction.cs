using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Broker.Transactions;

/// <summary>
/// An open transaction: an ordered list of buffered operations. Each operation
/// pairs a commit delegate with an optional rollback delegate and an optional
/// prepare check. The owning <see cref="TransactionManager"/> runs one set or the
/// other when the client discharges the transaction.
/// </summary>
internal sealed class Transaction
{
    private readonly object _gate = new();
    private readonly List<(Func<bool>? Prepare, Action Commit, Action? Rollback)> _operations = new();

    public void Enlist(Action commit, Action? rollback, Func<bool>? prepare = null)
    {
        ArgumentNullException.ThrowIfNull(commit);
        lock (_gate)
        {
            _operations.Add((prepare, commit, rollback));
        }
    }

    /// <summary>
    /// Applies the transaction atomically. First runs every operation's optional prepare
    /// check (e.g. "is this message's lock still held?"); if any fails, nothing is applied,
    /// the rollback actions run, and the method returns <c>false</c> so the coordinator can
    /// reject the discharge. Only when every operation can commit are the commit actions run,
    /// in enlist order. An in-memory broker cannot truly two-phase-commit, so a commit that
    /// still throws after a successful prepare is surfaced (returns <c>false</c>) rather than
    /// swallowed — a "committed" transaction must never silently drop an operation.
    /// </summary>
    public bool RunCommit(ILogger log)
    {
        List<(Func<bool>? Prepare, Action Commit, Action? Rollback)> ops;
        lock (_gate)
        {
            ops = new List<(Func<bool>?, Action, Action?)>(_operations);
        }

        // Phase 1 — prepare: validate every operation can commit before applying any.
        foreach (var op in ops)
        {
            if (op.Prepare is null) continue;

            bool canCommit;
            try
            {
                canCommit = op.Prepare();
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "A buffered transactional operation failed to prepare; rolling back the transaction.");
                canCommit = false;
            }

            if (canCommit) continue;

            log.LogWarning("A buffered transactional operation cannot be committed (e.g. a lost message lock); rolling back the transaction so the failure is surfaced to the client.");
            RunRollback(log);
            return false;
        }

        // Phase 2 — apply: every operation validated, so commit them in enlist order.
        var applied = true;
        foreach (var op in ops)
        {
            try
            {
                op.Commit();
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "A buffered transactional operation failed during commit after preparing successfully.");
                applied = false;
            }
        }

        return applied;
    }

    /// <summary>
    /// Runs every rollback action (in enlist order). Commit actions are discarded.
    /// Never throws.
    /// </summary>
    public void RunRollback(ILogger log)
    {
        List<(Func<bool>? Prepare, Action Commit, Action? Rollback)> ops;
        lock (_gate)
        {
            ops = new List<(Func<bool>?, Action, Action?)>(_operations);
        }

        foreach (var op in ops)
        {
            if (op.Rollback is null) continue;
            try
            {
                op.Rollback();
            }
            catch (Exception ex)
            {
                log.LogDebug(ex, "A buffered transactional rollback action failed; continuing.");
            }
        }
    }
}
