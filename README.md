
# Requirements
- Install uv
- Install ruff

## Install uv
### Mac/Linux
Use curl to download the script and execute it with sh:

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```
If your system doesn't have curl, you can use wget:

```shell
wget -qO- https://astral.sh/uv/install.sh | sh
```
Request a specific version by including it in the URL:

```shell
curl -LsSf https://astral.sh/uv/0.6.8/install.sh | sh
```
### Homebrew
```shell
brew install uv
```
## Install ruff
```shell
# Install Ruff globally.
uv tool install ruff@latest

# Or add Ruff to your project.
uv add --dev ruff

# With pip.
pip install ruff

# With pipx.
pipx install ruff
```

# Working locally

```shell
uv venv --python $(pyenv which python)
uv sync
source .venv/bin/activate
```
