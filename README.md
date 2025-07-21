# GoldenGate Deployment Project

This project provides Ansible playbooks for deploying and managing Oracle GoldenGate Extract, Distribution Path, and Replicat configurations using JSON-based configuration files with Jinja2 templating.

## Project Structure

```
project/
├── .github/
│   ├── workflows/
│   │   └── goldengate-deploy.yml   # GitHub Actions CI/CD workflow
│   └── ENVIRONMENT_SETUP.md        # GitHub environment setup guide
├── playbooks/
│   ├── goldengate-deployment.yml   # Main deployment playbook
│   └── goldengate-management.yml   # Management operations (start/stop)
├── config/
│   ├── extracts.json               # Extract configurations
│   ├── distpaths.json              # Distribution path configurations
│   └── replicats.json              # Replicat configurations
├── templates/
│   └── variables.yml.j2            # Jinja2 template variables
├── inventory/
│   ├── dev.yml                     # Development environment inventory
│   ├── uat.yml                     # UAT environment inventory
│   ├── prod.yml                    # Production environment inventory
│   └── hosts.yml                   # Default inventory file
├── ansible.cfg                     # Ansible configuration
└── README.md                       # This file
```

## Configuration Files

### JSON Configuration Structure

The configuration files use Jinja2 templating to allow dynamic values:

- **extracts.json**: Defines source database extracts with trail files
- **distpaths.json**: Defines distribution paths for remote trail delivery  
- **replicats.json**: Defines target database replicats

### Template Variables

Variables are defined in `templates/variables.yml.j2` and can be overridden in inventory or at runtime.

## Usage

### Local Development

```bash
# Deploy all components to development environment
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/dev.yml

# Deploy specific components with tags
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/dev.yml --tags extract
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/dev.yml --tags distpath  
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/dev.yml --tags replicat

# Manage components
ansible-playbook playbooks/goldengate-management.yml -i inventory/dev.yml -e "action=started"
ansible-playbook playbooks/goldengate-management.yml -i inventory/dev.yml -e "action=stopped component=extracts"
```

### CI/CD Deployment via GitHub Actions

The project includes GitHub Actions workflows for automated deployment across environments:

#### Automatic Deployments
- **Development**: Triggered on push to `develop` branch
- **UAT**: Triggered on push to `main` branch

#### Manual Deployments
- **Production**: Manual trigger via GitHub Actions with approval required
- **Any Environment**: Use workflow_dispatch for manual deployments

#### Environment Variables
The workflows use GitHub environment secrets:
- `GOLDENGATE_URL` - GoldenGate server endpoint
- `GOLDENGATE_USERNAME` - GoldenGate API username  
- `GOLDENGATE_PASSWORD` - GoldenGate API password

See `.github/ENVIRONMENT_SETUP.md` for detailed setup instructions.

### Environment-specific Variables

Each environment has its own inventory file with environment-specific configurations:

```bash
# Development
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/dev.yml

# UAT
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/uat.yml

# Production
ansible-playbook playbooks/goldengate-deployment.yml -i inventory/prod.yml
```

Override variables using extra vars:

```bash
# Using command-line variables
ansible-playbook playbooks/goldengate-deployment.yml \
  -i inventory/prod.yml \
  -e "source_host=prod-db.company.com" \
  -e "GOLDENGATE_URL=https://prod-ogg:9011"
```

## Dynamic Trail File Names

Trail file names are templated using Jinja2:

```json
{
  "trail_file": "{{ trail_file_prefix }}"
}
```

Variables like `trail_file_prefix` can be set to dynamic values like:
- Date/time stamps: `"et{{ ansible_date_time.epoch }}"`
- Environment names: `"{{ environment }}_et"`
- Custom patterns: `"{{ source_service | lower }}_trail"`

## Security Considerations

- Store sensitive information in GitHub environment secrets (CI/CD) or Ansible Vault (local)
- Use encrypted vault files for production deployments when running locally
- Set appropriate SSL certificate validation in production environments
- Implement proper approval workflows for production deployments
- Restrict GitHub environment access to authorized personnel

## GitHub Actions Setup

1. Create GitHub environments: `development`, `uat`, `production`
2. Configure required secrets for each environment (see `.github/ENVIRONMENT_SETUP.md`)
3. Set up production environment protection rules with required reviewers
4. Configure branch protection rules for automatic deployments

## Requirements

- Ansible 2.9+
- Oracle GoldenGate Collection (oracle.goldengate)
- Access to GoldenGate REST API endpoints
- Proper database credentials configured in GoldenGate
- GitHub repository with Actions enabled (for CI/CD)
- GitHub environment secrets configured for each target environment