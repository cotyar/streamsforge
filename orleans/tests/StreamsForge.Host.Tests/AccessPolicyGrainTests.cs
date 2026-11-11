using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;
using StreamsForge.Host.Storage;

namespace StreamsForge.Host.Tests;

/// <summary>Memory grain storage only — this grain touches no streams at all, but the silo still needs
/// the "definitions" store its <c>[PersistentState]</c> names.</summary>
internal sealed class AccessPolicyTestSiloConfigurator : ISiloConfigurator
{
    public void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.AddJsonMemoryGrainStorage(StreamConstants.StorageName);
}

internal sealed class AccessPolicyTestClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) { }
}

/// <summary>Plan 015 W1 — the Orleans access-policy store, against a real TestingHost cluster (grain
/// discovery by assembly scan, exactly as the Host does it; no Program.cs involvement).
///
/// <para>Every assertion here is a semantic the Dapr twin (<c>AccessPolicyActor</c>/
/// <c>AccessPolicyStore</c>) has to reproduce byte for byte, so these read as a specification and not
/// as coverage: the version-bump discipline the resolver's TTL polling depends on, the built-in-role
/// delete refusal that stops an operator stranding every pre-upgrade token, and the empty-name
/// rejection that keeps unaddressable records out of a document the resolver walks on every request.
/// Each test uses its OWN grain key so the cases stay independent — the real deployment has exactly one
/// activation under <see cref="StreamConstants.AccessKey"/>, which is asserted separately.</para></summary>
public sealed class AccessPolicyGrainTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<AccessPolicyTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<AccessPolicyTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private IAccessPolicyGrain Grain([System.Runtime.CompilerServices.CallerMemberName] string key = "") =>
        _cluster.GrainFactory.GetGrain<IAccessPolicyGrain>("access-test-" + key);

    private static PermissionGrant Grant(string action, string scope = "*") =>
        new() { Action = action, Scope = scope };

    // ------------------------------------------------------------------------------------------
    // Empty / never-written document
    // ------------------------------------------------------------------------------------------

    /// <summary>A never-written grain is a legitimate state, not an error: an empty document at version
    /// 0, never null. Every pre-upgrade data dir starts exactly here, and seeding is somebody else's
    /// job (LegacyRoleMigration), so a store that threw or returned null on first read would break the
    /// upgrade path before a single role existed.</summary>
    [Fact]
    public async Task NeverWritten_ReturnsEmptyDocumentAtVersionZero()
    {
        var doc = await Grain().GetPolicyAsync();

        Assert.NotNull(doc);
        Assert.Equal(0, doc.Version);
        Assert.Empty(doc.Roles);
        Assert.Empty(doc.Groups);
        Assert.Empty(doc.Users);
        Assert.Empty(doc.ApprovalTemplates);
        Assert.Equal(0, await Grain().GetVersionAsync());
    }

    /// <summary>GetVersionAsync is called by every replica every Auth:PolicyCacheSeconds. Polling it
    /// must never move anything — if the poll itself were a mutation, every replica would invalidate
    /// every other replica's cache forever.</summary>
    [Fact]
    public async Task GetVersion_HasNoSideEffects()
    {
        var grain = Grain();
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor" }, "admin");

        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(1, await grain.GetVersionAsync());
        }

        Assert.Equal(1, (await grain.GetPolicyAsync()).Version);
    }

    // ------------------------------------------------------------------------------------------
    // The version-bump contract the resolver's TTL polling is built on
    // ------------------------------------------------------------------------------------------

    /// <summary>Exactly one per mutation, across all four record types, in order. The resolver refetches
    /// only when this number moves, so a mutation that forgot to bump would leave a revoked entitlement
    /// live until something else happened to write.</summary>
    [Fact]
    public async Task EveryMutation_BumpsVersionByExactlyOne()
    {
        var grain = Grain();

        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor" }, "admin");
        Assert.Equal(1, await grain.GetVersionAsync());

        await grain.UpsertGroupAsync(new GroupDefinition { Name = "risk" }, "admin");
        Assert.Equal(2, await grain.GetVersionAsync());

        await grain.UpsertUserAccessAsync(new UserAccessEntry { Username = "alice" }, "admin");
        Assert.Equal(3, await grain.GetVersionAsync());

        await grain.UpsertApprovalTemplateAsync(new ApprovalTemplate { Name = "prod-deploy" }, "admin");
        Assert.Equal(4, await grain.GetVersionAsync());

        Assert.True(await grain.DeleteApprovalTemplateAsync("prod-deploy"));
        Assert.Equal(5, await grain.GetVersionAsync());

        Assert.True(await grain.DeleteUserAccessAsync("alice"));
        Assert.Equal(6, await grain.GetVersionAsync());

        Assert.True(await grain.DeleteGroupAsync("risk"));
        Assert.Equal(7, await grain.GetVersionAsync());

        Assert.True(await grain.DeleteRoleAsync("Auditor"));
        Assert.Equal(8, await grain.GetVersionAsync());
    }

    /// <summary>ponytail, asserted rather than merely commented: a re-upsert of identical content still
    /// bumps, costing every replica one spurious refetch. Pinning it means the day someone adds the
    /// equality short-circuit, this test fails and forces the decision to be made deliberately instead
    /// of drifting apart from the Dapr twin.</summary>
    [Fact]
    public async Task IdenticalUpsert_StillBumpsVersion()
    {
        var grain = Grain();
        var role = new RoleDefinition { Name = "Auditor", Grants = [Grant(Actions.AuditRead)] };

        await grain.UpsertRoleAsync(role, "admin");
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor", Grants = [Grant(Actions.AuditRead)] }, "admin");

        Assert.Equal(2, await grain.GetVersionAsync());
        Assert.Single((await grain.GetPolicyAsync()).Roles);
    }

    /// <summary>A failed mutation must be a complete no-op — no bump, no write. Otherwise every replica
    /// pays a refetch for a document that did not change, on the one path (a bad request) that is
    /// cheapest to trigger from outside.</summary>
    [Fact]
    public async Task RejectedAndAbsentMutations_DoNotBumpVersion()
    {
        var grain = Grain();
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor" }, "admin");

        Assert.Null(await grain.UpsertRoleAsync(new RoleDefinition { Name = "" }, "admin"));
        Assert.Null(await grain.UpsertGroupAsync(new GroupDefinition { Name = "" }, "admin"));
        Assert.Null(await grain.UpsertUserAccessAsync(new UserAccessEntry { Username = "" }, "admin"));
        Assert.Null(await grain.UpsertApprovalTemplateAsync(new ApprovalTemplate { Name = "" }, "admin"));
        Assert.False(await grain.DeleteRoleAsync("nope"));
        Assert.False(await grain.DeleteGroupAsync("nope"));
        Assert.False(await grain.DeleteUserAccessAsync("nope"));
        Assert.False(await grain.DeleteApprovalTemplateAsync("nope"));

        Assert.Equal(1, await grain.GetVersionAsync());
        var doc = await grain.GetPolicyAsync();
        Assert.Single(doc.Roles);
        Assert.Empty(doc.Groups);
        Assert.Empty(doc.Users);
        Assert.Empty(doc.ApprovalTemplates);
    }

    // ------------------------------------------------------------------------------------------
    // Roles
    // ------------------------------------------------------------------------------------------

    /// <summary>An upsert stamps the record's own UpdatedAtMs/UpdatedBy from the actor argument and the
    /// document's UpdatedAtMs, and returns the STORED copy — the SPA renders "changed by X at T" off
    /// exactly these fields, and a caller-supplied UpdatedBy would let anyone attribute a change to
    /// anyone.</summary>
    [Fact]
    public async Task UpsertRole_StampsActorAndTimestamps_AndReturnsStoredCopy()
    {
        var grain = Grain();
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var stored = await grain.UpsertRoleAsync(
            new RoleDefinition { Name = "Auditor", UpdatedBy = "somebody-else", Grants = [Grant(Actions.AuditRead)] },
            "admin");

        Assert.NotNull(stored);
        Assert.Equal("admin", stored.UpdatedBy);
        Assert.True(stored.UpdatedAtMs >= before);

        var doc = await grain.GetPolicyAsync();
        var fromDoc = Assert.Single(doc.Roles);
        Assert.Equal("Auditor", fromDoc.Name);
        Assert.Equal("admin", fromDoc.UpdatedBy);
        Assert.Equal(stored.UpdatedAtMs, fromDoc.UpdatedAtMs);
        Assert.True(doc.UpdatedAtMs >= before);
        Assert.Equal(Actions.AuditRead, Assert.Single(fromDoc.Grants).Action);
    }

    /// <summary>Upsert replaces the matched record in place rather than appending a second one — the
    /// evaluator walks this list and two records with one name would make the decision depend on list
    /// order.</summary>
    [Fact]
    public async Task UpsertRole_ReplacesByNameInPlace()
    {
        var grain = Grain();
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "A" }, "admin");
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor", Grants = [Grant(Actions.AuditRead)] }, "admin");
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Z" }, "admin");

        await grain.UpsertRoleAsync(
            new RoleDefinition { Name = "Auditor", Description = "narrowed", Grants = [] },
            "editor");

        var roles = (await grain.GetPolicyAsync()).Roles;
        Assert.Equal(["A", "Auditor", "Z"], roles.Select(r => r.Name));
        var auditor = roles[1];
        Assert.Equal("narrowed", auditor.Description);
        Assert.Empty(auditor.Grants);
        Assert.Equal("editor", auditor.UpdatedBy);
    }

    /// <summary>Matching is ordinal and case-SENSITIVE: "auditor" and "Auditor" are two roles. Pinned
    /// because the Dapr twin has to agree — a flavour that folded case would silently merge two
    /// entitlement sets that the other flavour keeps apart.</summary>
    [Fact]
    public async Task UpsertRole_MatchesCaseSensitively()
    {
        var grain = Grain();
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor" }, "admin");
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "auditor" }, "admin");

        Assert.Equal(2, (await grain.GetPolicyAsync()).Roles.Count);
    }

    /// <summary>The single most consequential refusal in this file. Viewer/Editor/Admin cannot be
    /// deleted at all: a pre-upgrade token's ONLY authorization claim is its role string, so deleting
    /// Viewer would strand every one of them the moment the resolver stopped falling back.</summary>
    [Theory]
    [InlineData(BuiltInRoles.Admin)]
    [InlineData(BuiltInRoles.Editor)]
    [InlineData(BuiltInRoles.Viewer)]
    public async Task DeleteRole_RefusesBuiltIns_EvenWhenStored(string name)
    {
        var grain = _cluster.GrainFactory.GetGrain<IAccessPolicyGrain>("access-builtin-" + name);
        await grain.UpsertRoleAsync(new RoleDefinition { Name = name, BuiltIn = true }, "admin");

        Assert.False(await grain.DeleteRoleAsync(name));

        var doc = await grain.GetPolicyAsync();
        Assert.Equal(name, Assert.Single(doc.Roles).Name);
        Assert.Equal(1, doc.Version); // the refusal did not bump
    }

    /// <summary>Built-ins may be EDITED — an operator narrowing Editor is legitimate and reversible;
    /// only removal is refused.</summary>
    [Fact]
    public async Task UpsertRole_AllowsEditingABuiltIn()
    {
        var grain = Grain();
        var stored = await grain.UpsertRoleAsync(
            new RoleDefinition { Name = BuiltInRoles.Editor, BuiltIn = true, Grants = [Grant(Actions.CatalogRead)] },
            "admin");

        Assert.NotNull(stored);
        Assert.Equal(Actions.CatalogRead, Assert.Single(stored.Grants).Action);
        Assert.True(stored.BuiltIn);
    }

    // ------------------------------------------------------------------------------------------
    // Groups
    // ------------------------------------------------------------------------------------------

    /// <summary>CreatedAtMs is set on create and carried forward on update — it belongs to the group,
    /// not to the write, so a PUT built from a partially-filled DTO cannot reset a group's age. Returning
    /// the stored copy is what makes that override visible instead of a surprise on the next GET.</summary>
    [Fact]
    public async Task UpsertGroup_SetsCreatedAtOnCreate_AndPreservesItOnUpdate()
    {
        var grain = Grain();
        var created = await grain.UpsertGroupAsync(
            new GroupDefinition { Name = "risk", Members = ["alice"] }, "admin");

        Assert.NotNull(created);
        Assert.True(created.CreatedAtMs > 0);

        var updated = await grain.UpsertGroupAsync(
            new GroupDefinition { Name = "risk", Members = ["alice", "bob"], CreatedAtMs = 0 }, "editor");

        Assert.NotNull(updated);
        Assert.Equal(created.CreatedAtMs, updated.CreatedAtMs);
        Assert.Equal(["alice", "bob"], updated.Members);
        Assert.Equal("editor", updated.UpdatedBy);
        Assert.Single((await grain.GetPolicyAsync()).Groups);
    }

    /// <summary>Deleting a group rewrites NO user record. Membership lives on the group, so there is
    /// nothing dangling to clean up — and a user-list rewrite here would be the second whole-list-rewrite
    /// path the storage decision exists to avoid.</summary>
    [Fact]
    public async Task DeleteGroup_LeavesUserEntriesUntouched()
    {
        var grain = Grain();
        await grain.UpsertGroupAsync(new GroupDefinition { Name = "risk", Members = ["alice"] }, "admin");
        var user = await grain.UpsertUserAccessAsync(
            new UserAccessEntry { Username = "alice", Roles = [BuiltInRoles.Viewer] }, "admin");
        Assert.NotNull(user);

        Assert.True(await grain.DeleteGroupAsync("risk"));

        var doc = await grain.GetPolicyAsync();
        Assert.Empty(doc.Groups);
        var alice = Assert.Single(doc.Users);
        Assert.Equal("alice", alice.Username);
        Assert.Equal([BuiltInRoles.Viewer], alice.Roles);
        Assert.Equal(user.UpdatedAtMs, alice.UpdatedAtMs); // not re-stamped by the group delete
    }

    /// <summary>The OIDC seam stored verbatim: when OIDC lands, the resolver takes membership from both
    /// the store and the IdP's groups claim, and the mapping is already persisted.</summary>
    [Fact]
    public async Task UpsertGroup_RoundTripsExternalClaimValues()
    {
        var grain = Grain();
        await grain.UpsertGroupAsync(
            new GroupDefinition
            {
                Name = "risk",
                Roles = [BuiltInRoles.Editor],
                Grants = [Grant(Actions.PipelineControl, "prod-*")],
                ExternalClaimValues = ["cn=risk,ou=groups"],
            },
            "admin");

        var group = Assert.Single((await grain.GetPolicyAsync()).Groups);
        Assert.Equal(["cn=risk,ou=groups"], group.ExternalClaimValues);
        Assert.Equal("prod-*", Assert.Single(group.Grants).Scope);
    }

    // ------------------------------------------------------------------------------------------
    // User access entries
    // ------------------------------------------------------------------------------------------

    /// <summary>The mirror path: the user store calls this on every create/update to copy
    /// UserRecord.Role into UserAccessEntry.Roles, which is what makes a role change take effect within
    /// the resolver's TTL instead of at the next login. Create-then-update through the same call.</summary>
    [Fact]
    public async Task UpsertUserAccess_CreatesThenUpdatesByUsername()
    {
        var grain = Grain();
        await grain.UpsertUserAccessAsync(
            new UserAccessEntry { Username = "alice", Roles = [BuiltInRoles.Viewer] }, "system");

        var updated = await grain.UpsertUserAccessAsync(
            new UserAccessEntry
            {
                Username = "alice",
                Roles = [BuiltInRoles.Editor],
                Grants = [new PermissionGrant { Action = Actions.PipelineWrite, Scope = "prod-*", Effect = PermissionEffect.Deny }],
            },
            "admin");

        Assert.NotNull(updated);
        var alice = Assert.Single((await grain.GetPolicyAsync()).Users);
        Assert.Equal([BuiltInRoles.Editor], alice.Roles);
        Assert.Equal(PermissionEffect.Deny, Assert.Single(alice.Grants).Effect);
        Assert.Equal("admin", alice.UpdatedBy);
    }

    /// <summary>Disabling is the cheap 90% of token revocation: the resolver returns an empty grant set
    /// for a disabled user, so the flag has to survive the round trip and move the version.</summary>
    [Fact]
    public async Task UpsertUserAccess_PersistsDisabledFlag()
    {
        var grain = Grain();
        await grain.UpsertUserAccessAsync(new UserAccessEntry { Username = "alice" }, "admin");
        await grain.UpsertUserAccessAsync(new UserAccessEntry { Username = "alice", Disabled = true }, "admin");

        var doc = await grain.GetPolicyAsync();
        Assert.True(Assert.Single(doc.Users).Disabled);
        Assert.Equal(2, doc.Version);
    }

    // ------------------------------------------------------------------------------------------
    // Approval templates
    // ------------------------------------------------------------------------------------------

    /// <summary>Templates ship seeded but inert (Approvals:Enabled=false); the store keeps their whole
    /// shape — N-of-M, expiry and escalation — verbatim for the sweeper to read later.</summary>
    [Fact]
    public async Task UpsertApprovalTemplate_RoundTripsAndReplacesByName()
    {
        var grain = Grain();
        await grain.UpsertApprovalTemplateAsync(
            new ApprovalTemplate
            {
                Name = "prod-deploy",
                ActionPattern = "pipeline.*",
                ScopePattern = "prod-*",
                RequiredApprovals = 2,
                ApproverGroups = ["risk"],
                EscalateAfterSeconds = 600,
                EscalationGroups = ["oncall"],
            },
            "admin");

        await grain.UpsertApprovalTemplateAsync(
            new ApprovalTemplate { Name = "prod-deploy", ActionPattern = "pipeline.*", RequiredApprovals = 3, Enabled = false },
            "admin");

        var template = Assert.Single((await grain.GetPolicyAsync()).ApprovalTemplates);
        Assert.Equal(3, template.RequiredApprovals);
        Assert.False(template.Enabled);
        Assert.Empty(template.ApproverGroups); // replaced wholesale, not merged
    }

    // ------------------------------------------------------------------------------------------
    // Persistence + the singleton key
    // ------------------------------------------------------------------------------------------

    /// <summary>Everything is persisted, not activation-local: a deactivated grain rehydrates the whole
    /// document, version included. Without this the resolver's version poll would reset to 0 on every
    /// activation-collection and every replica would refetch a document that had not changed.</summary>
    [Fact]
    public async Task State_SurvivesDeactivation()
    {
        var grain = Grain();
        await grain.UpsertRoleAsync(new RoleDefinition { Name = "Auditor", Grants = [Grant(Actions.AuditRead)] }, "admin");
        await grain.UpsertGroupAsync(new GroupDefinition { Name = "risk", Members = ["alice"] }, "admin");
        await grain.UpsertUserAccessAsync(new UserAccessEntry { Username = "alice", Disabled = true }, "admin");

        // The repo's established idiom (ShardedTableClusterTests.CollectIdleActivationsAsync): force a
        // real deactivation rather than waiting one out. This grain pins nothing, so it is collected and
        // the next call re-reads the document from storage.
        await _cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

        var doc = await Grain().GetPolicyAsync();
        Assert.Equal(3, doc.Version);
        Assert.Equal("Auditor", Assert.Single(doc.Roles).Name);
        Assert.Equal(["alice"], Assert.Single(doc.Groups).Members);
        Assert.True(Assert.Single(doc.Users).Disabled);
    }

    /// <summary>The deployment addresses exactly one activation, under StreamConstants.AccessKey — the
    /// key the DI registration in OrleansFacades hands to GetGrain. Asserted here because a typo'd key
    /// would produce a silently empty, perfectly healthy-looking policy on a running cluster.</summary>
    [Fact]
    public async Task SingletonKey_IsTheOneTheFacadeUses()
    {
        var byConstant = _cluster.GrainFactory.GetGrain<IAccessPolicyGrain>(StreamConstants.AccessKey);
        await byConstant.UpsertRoleAsync(new RoleDefinition { Name = "SingletonProbe" }, "admin");

        var byLiteral = _cluster.GrainFactory.GetGrain<IAccessPolicyGrain>("access");
        Assert.Equal("SingletonProbe", Assert.Single((await byLiteral.GetPolicyAsync()).Roles).Name);
    }
}
