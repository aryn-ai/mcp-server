# Aryn Local MCP Server

## Installation

### Prerequisites
* Python 3.12 or higher. Install it [here](https://www.python.org/downloads/)
* [uv](https://github.com/astral-sh/uv) - Fast Python package installer and resolver (see installation instructions below)
* An Aryn API key. You can create an account and receive a key for free [here](https://app.aryn.ai/)

### Installing uv

This project uses `uvx` for easy execution. To get started, you need to install `uv` first, which provides the `uvx` command:

1. Install uv for macOS/Linux (if not already installed):
   ```
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install uv for Windows (if not already installed):
   ```
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

After installation, you'll have access to both `uv` and `uvx` commands. The `uvx` command is what you'll use to run this MCP server.

### One-Click Install for Claude Desktop (Claude Desktop Only)

Instead of manually installing this MCP server, Claude Desktop allows for an easy one-click extension:

1. **Download the extension**: Retrieve the provided `.dxt` file from this repository
2. **Install the extension**: Drag the `.dxt` file into Claude Desktop's Settings window
3. **Review and install**: You'll see:
   - Human-readable information about your extension
   - Required permissions and configuration
   - A simple "Install" button

For more details, refer to the [Claude Desktop Extensions documentation](https://www.anthropic.com/engineering/desktop-extensions).

### Configuration

If you're manually installing this MCP server, add the following configuration to your MCP client config file

```json
{
  "mcpServers": {
    "Aryn SDK Local": {
      "command": "uvx",
      "args": [
        "https://pypi.org/simple/",
        "aryn-mcp-server"
      ],
      "env": {
        "ARYN_API_KEY": "YOUR_ARYN_API_KEY",
        "ARYN_MCP_OUTPUT_DIR": "<full path to directory where files will get saved (ie Users/username/Downloads)>"
      }
    }
  }
}
```

For client specific config implementation, see below:
* [Claude](https://docs.anthropic.com/en/docs/claude-code/mcp#use-mcp-prompts-as-slash-commands)
* [Cursor](https://docs.cursor.com/en/context/mcp)

### Troubleshooting

If you encounter `spawn uvx ENOENT` errors:

1. **Verify uv installation**: Run `which uvx` in your terminal to find the correct path

2. **Use the full path to uv**: Replace `"command": "uvx"` with `"command": "<full path to uvx>"`
