#!/bin/bash

# Define the Rosetta version
ROSETTA_VERSION="__ROSETTA_VERSION__"

# Determine the OS and Architecture
OS=$(uname -s)
ARCH=$(uname -m)

# Initialize variables
URL=""
ZIP_FILE=""
EXTRACTION_DIR=""

# Set URL, ZIP_FILE, and EXTRACTION_DIR based on OS and ARCH
if [[ "$OS" == "Darwin" ]]; then
    if [[ "$ARCH" == "arm64" ]]; then
        echo "Running on macOS (ARM architecture)"
        ZIP_FILE="rosetta-${ROSETTA_VERSION}-mac_aarch64.zip"
        EXTRACTION_DIR="rosetta-${ROSETTA_VERSION}-mac_aarch64"
    elif [[ "$ARCH" == "x86_64" ]]; then
        echo "Running on macOS (Intel architecture)"
        ZIP_FILE="rosetta-${ROSETTA_VERSION}-mac_x64.zip"
        EXTRACTION_DIR="rosetta-${ROSETTA_VERSION}-mac_x64"
    else
        echo "Unknown architecture on macOS: $ARCH"
        exit 1
    fi
elif [[ "$OS" == "Linux" ]]; then
    echo "Running on Linux"
    if [[ "$ARCH" == "x86_64" ]]; then
        ZIP_FILE="rosetta-${ROSETTA_VERSION}-linux_x64.zip"
        EXTRACTION_DIR="rosetta-${ROSETTA_VERSION}-linux_x64"
        URL="https://github.com/AdaptiveScale/rosetta/releases/download/v${ROSETTA_VERSION}/${ZIP_FILE}"
    elif [[ "$ARCH" == "arm64" ]]; then
        ZIP_FILE="rosetta-${ROSETTA_VERSION}-linux_aarch64.zip"
        EXTRACTION_DIR="rosetta-${ROSETTA_VERSION}-linux_aarch64"
    else
        echo "Unknown architecture on Linux: $ARCH"
        exit 1
    fi
else
    echo "Unsupported OS: $OS"
    exit 1
fi


URL="https://github.com/AdaptiveScale/rosetta/releases/download/v${ROSETTA_VERSION}/${ZIP_FILE}"

# Download the ZIP file using wget
echo "Downloading $ZIP_FILE..."
wget -q --show-progress "$URL" -O "$ZIP_FILE"

# Check if the download was successful
if [ $? -eq 0 ]; then
    echo "Download complete."
else
    echo "Download failed. Exiting."
    exit 1
fi

# Extract the ZIP file
echo "Extracting $ZIP_FILE..."
unzip -q "$ZIP_FILE" -d "$EXTRACTION_DIR"

# Check if the extraction was successful
if [ $? -eq 0 ]; then
    echo "Extraction complete. Files are in $EXTRACTION_DIR."
else
    echo "Extraction failed. Exiting."
    exit 1
fi

# Cleanup: Remove the ZIP file (optional)
echo "Cleaning up..."
rm -f "$ZIP_FILE"

# Define the directory to add to PATH
EXTRACTION_DIR="$PWD/$EXTRACTION_DIR/$EXTRACTION_DIR/bin"

# Check if the directory exists
if [ -d "$EXTRACTION_DIR" ]; then
    echo "Adding $EXTRACTION_DIR to PATH..."
else
    echo "Directory $EXTRACTION_DIR does not exist. Exiting."
    exit 1
fi

# Check if the directory exists
if [ ! -d "$EXTRACTION_DIR" ]; then
    echo "Directory $EXTRACTION_DIR does not exist. Exiting."
    exit 1
fi

# Detect the appropriate shell configuration file
SHELL_CONFIG=""
if [ -n "$ZSH_VERSION" ]; then
    SHELL_CONFIG="$HOME/.zshrc"
elif [ -n "$BASH_VERSION" ]; then
    SHELL_CONFIG="$HOME/.bashrc"
fi

if [ -z "$SHELL_CONFIG" ]; then
    echo "Could not determine shell configuration file. Add the following line manually:"
    echo "export PATH=\"$EXTRACTION_DIR:\$PATH\""
    exit 1
fi

# Backup the shell configuration file
echo "Creating a backup of $SHELL_CONFIG..."
cp "$SHELL_CONFIG" "$SHELL_CONFIG.bak"

# Check for an existing Rosetta PATH line and replace it if found
if grep -q "export PATH=.*rosetta" "$SHELL_CONFIG"; then
    echo "Replacing existing Rosetta PATH in $SHELL_CONFIG..."
    sed -i.bak "/export PATH=.*rosetta/d" "$SHELL_CONFIG"
else
    echo "No existing Rosetta PATH found in $SHELL_CONFIG."
fi

# Add the new PATH line for Rosetta
echo "Adding new PATH for Rosetta to $SHELL_CONFIG..."
echo "export PATH=\"$EXTRACTION_DIR:\$PATH\"" >> "$SHELL_CONFIG"

# echo "PATH updated. Restart your terminal or run 'source $SHELL_CONFIG' to apply."

# Apply the changes to the current session
echo "Sourcing $SHELL_CONFIG..."
source "$SHELL_CONFIG"

# Prompt for project name with a default value
DEFAULT_PROJECT_NAME="my_rosetta_project"
read -p "Enter the project name (default: $DEFAULT_PROJECT_NAME): " PROJECT_NAME
PROJECT_NAME=${PROJECT_NAME:-$DEFAULT_PROJECT_NAME}

# Run the Rosetta init command
echo "Initializing Rosetta project: $PROJECT_NAME..."
rosetta init "$PROJECT_NAME"

if [ $? -eq 0 ]; then
    echo "Rosetta project '$PROJECT_NAME' initialized successfully."
    echo "Next steps:"
    echo "1. Navigate to the project directory: cd $PROJECT_NAME"
    echo "2. Configure your database connections in the 'main.conf' file inside the project directory."
    echo "3. Start using Rosetta commands"
else
    echo "Failed to initialize Rosetta project. Please check for errors."
fi