#!/bin/bash

set -e

OCTOTS_URL="https://raw.githubusercontent.com/OctoTS/OctoTS-tools/refs/heads/main/OctoTS.py"
REQUIREMENTS_URL="https://raw.githubusercontent.com/OctoTS/OctoTS-tools/refs/heads/main/requirements.txt"

echo "🐙 Starting OctoTS Installation..."

if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is required but not installed."
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo "❌ Error: curl is required to download the script."
    echo "Please install curl and try again (e.g., sudo apt install curl)."
    exit 1
fi

INSTALL_DIR="$HOME/.octots"
BIN_DIR="$HOME/.local/bin"
VENV_DIR="$INSTALL_DIR/venv"

echo "📁 Creating installation directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$BIN_DIR"

echo "🐍 Creating an isolated Python virtual environment..."
if ! python3 -m venv "$VENV_DIR"; then
    echo "❌ Error: Failed to create a virtual environment."
    echo "On Debian/Ubuntu, you likely need to install the venv package first:"
    echo "👉 Run: sudo apt install python3-venv"
    echo "Then try this installer again."
    exit 1
fi

echo "⬇️ Downloading requirements.txt from GitHub..."
if ! curl -sL -f "$REQUIREMENTS_URL" -o "$INSTALL_DIR/requirements.txt"; then
    echo "❌ Error: Failed to download requirements.txt. Please make sure the file exists in the repository."
    exit 1
fi

echo "📦 Installing dependencies from requirements.txt safely..."
"$VENV_DIR/bin/pip" install -r "$INSTALL_DIR/requirements.txt" --quiet

echo "⬇️ Downloading latest OctoTS from GitHub..."
if ! curl -sL -f "$OCTOTS_URL" -o "$INSTALL_DIR/octots.py"; then
    echo "❌ Error: Failed to download OctoTS. Please check the GitHub URL in the script."
    exit 1
fi

WRAPPER="$BIN_DIR/octots"
echo "⚙️ Creating executable command..."

cat << EOF > "$WRAPPER"
#!/bin/bash
"$VENV_DIR/bin/python" "$INSTALL_DIR/octots.py" "\$@"
EOF

chmod +x "$WRAPPER"

echo ""
echo "✅ Installation complete! OctoTS has been installed successfully."
echo "--------------------------------------------------------"
echo "You can now start the interactive shell from anywhere by typing:"
echo "  octots"
echo ""
echo "If your terminal says 'command not found', you may need to add $BIN_DIR to your PATH by adding this line to your ~/.bashrc or ~/.zshrc:"
echo 'export PATH="$HOME/.local/bin:$PATH"'
echo "Then, restart your terminal."