# Plan: Azure CLI with Project-Scoped SPN

## Problem

The primary user (me) has Global Admin privileges in Entra ID. Running `az login` as
myself inside the devcontainer gives the terminal full admin reach. The goal is to
restrict what the container session *can* do, regardless of who is operating it.

## Decision

Use a single project-scoped Service Principal (SPN) for the devcontainer. This is not
about identity (who) — it is about capability restriction (what the terminal can do).

## Design

### Azure Side (one-time setup, Terraform optional)

1. Create an App Registration: `spn-dashboards-devcontainer`
2. Create a client secret (or federated credential if moving to OIDC later)
3. Assign RBAC roles scoped to only the resource groups this project needs:
   - Example: `Reader` on `rg-dashboards-prod`
   - Example: `Contributor` on `rg-dashboards-dev`
   - Example: `Storage Blob Data Reader` on specific storage accounts
4. No Entra directory roles — the SPN should have zero directory-level permissions

### Credential Storage

- Store `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_CLIENT_SECRET` in a
  `.env` file at the repo root (gitignored)
- Alternative: pull from Azure Key Vault or a local secret manager at container start

### Devcontainer Changes

#### Dockerfile

Add Azure CLI installation after the existing tooling block:

```dockerfile
# Install Azure CLI (scoped SPN auth only — do not use personal credentials)
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash
```

#### devcontainer.json

Add the `.env` file mount and auto-login:

```jsonc
// Add to containerEnv
"AZURE_CLIENT_ID": "${localEnv:AZURE_CLIENT_ID}",
"AZURE_TENANT_ID": "${localEnv:AZURE_TENANT_ID}",
"AZURE_CLIENT_SECRET": "${localEnv:AZURE_CLIENT_SECRET}"
```

Update `postStartCommand` to chain the SPN login:

```jsonc
"postStartCommand": "sudo /usr/local/bin/init-firewall.sh && az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID --output none 2>/dev/null || echo 'Azure SPN login skipped — credentials not set'"
```

#### .gitignore

Ensure `.env` is gitignored (if not already).

### Permission Maintenance

- RBAC assignments are defined per-resource-group, not per-user — the surface is small
  and stable
- When the project needs access to a new resource, add a role assignment to the SPN
- This can live in Terraform as a small module or be done manually — there is only one
  SPN to manage, not one per developer
- Periodically review with: `az role assignment list --assignee <spn-object-id> -o table`

## What This Does NOT Cover

- Per-developer audit trail in Azure logs (all actions show as the SPN)
- If per-developer auditing becomes a requirement, extend to one SPN per developer at
  that point — but do not pre-build that complexity now

## Risks

- Client secret rotation: set a calendar reminder or use Key Vault with auto-rotation
- If the `.env` file leaks, the blast radius is limited to the scoped RBAC assignments
- The SPN has no directory roles, so it cannot modify Entra ID itself

## Sequence

1. Create App Registration + SPN in Azure
2. Assign scoped RBAC roles
3. Generate client secret, store in `.env`
4. Update Dockerfile to install Azure CLI
5. Update devcontainer.json to pass env vars and auto-login
6. Rebuild container, verify with `az account show` and `az group list`
