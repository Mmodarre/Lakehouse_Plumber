#!/usr/bin/env bash

#==============================================================================
# Script: reset_testing_project.sh
# Description: Cleans up and resets the testing project directory with 
#              optional baseline operations
# Usage: ./reset_testing_project.sh [OPTIONS]
#==============================================================================

set -e  # Exit on error
set -u  # Exit on undefined variable

#------------------------------------------------------------------------------
# Configuration
#------------------------------------------------------------------------------
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly TESTING_PROJECT_DIR="${PROJECT_ROOT}/tests/e2e/fixtures/testing_project"

# Color codes for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

#------------------------------------------------------------------------------
# Global Variables
#------------------------------------------------------------------------------
DRY_RUN=false
MOVE_GENERATED=false
EMPTY_RESOURCES=false
MOVE_RESOURCES=false
EMPTY_GENERATED=false
NO_ARGS_PASSED=true

#------------------------------------------------------------------------------
# Functions
#------------------------------------------------------------------------------

# Print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Show usage information
show_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Reset and clean up the testing project directory.

DEFAULT BEHAVIOR (NO OPTIONS):
  Runs in DRY-RUN mode with:
  - Empty resources directory
  - Empty generated directory
  - Delete .lhp directory
  - Delete .lhp_state.json file
  - Git discard changes in databricks.yml

ALWAYS EXECUTED OPERATIONS:
  - Delete .lhp directory
  - Delete .lhp_state.json file
  - Git discard changes in databricks.yml

OPTIONAL OPERATIONS:
  --move-generated      Move generated/dev to generated_baseline/dev (overwrite)
  --empty-generated     Empty the generated directory
  --empty-resources     Empty the resources directory
  --move-resources    Move resources content to resources_baseline (overwrite)

OTHER OPTIONS:
  --dry-run            Show what would be executed without making changes
  --execute            Actually execute operations (overrides default dry-run)
  -h, --help           Show this help message

EXAMPLES:
  # Default: dry-run preview (empty resources + generated + always-run ops)
  $(basename "$0")

  # Actually execute the default operations
  $(basename "$0") --execute

  # Cleanup and move generated files to baseline
  $(basename "$0") --move-generated --execute

  # Cleanup and move resources
  $(basename "$0") --move-resources --execute

  # Custom dry run
  $(basename "$0") --move-generated --move-resources --dry-run

NOTES:
  - --empty-resources and --move-resources are mutually exclusive
  - When no options provided, runs in dry-run mode by default
  - Use --execute to actually perform operations
  - Script must be run from project root or will auto-detect location

EOF
}

# Execute or print command based on dry-run flag
execute_cmd() {
    local description="$1"
    shift
    local cmd="$*"
    
    if [[ "$DRY_RUN" == true ]]; then
        echo -e "${YELLOW}[DRY RUN]${NC} $description"
        echo "  Command: $cmd"
        return 0
    else
        print_info "$description"
        eval "$cmd"
        return 0
    fi
}

# Validate that testing project directory exists
validate_environment() {
    if [[ ! -d "$TESTING_PROJECT_DIR" ]]; then
        print_error "Testing project directory not found: $TESTING_PROJECT_DIR"
        print_error "Please run this script from the project root or ensure the path is correct."
        exit 1
    fi
    
    print_info "Working directory: $TESTING_PROJECT_DIR"
    
    # Check for mutually exclusive options
    if [[ "$EMPTY_RESOURCES" == true && "$MOVE_RESOURCES" == true ]]; then
        print_error "Cannot use both --empty-resources and --move-resources together"
        show_usage
        exit 1
    fi
}

# Delete .lhp directory
delete_lhp_dir() {
    local lhp_dir="${TESTING_PROJECT_DIR}/.lhp"
    
    if [[ -d "$lhp_dir" ]]; then
        execute_cmd "Deleting .lhp directory" "rm -rf '$lhp_dir'"
        if [[ "$DRY_RUN" == false ]]; then
            print_success "Deleted .lhp directory"
        fi
    else
        print_info ".lhp directory does not exist (skipping)"
    fi
}

# Delete .lhp_state.json file
delete_state_file() {
    local state_file="${TESTING_PROJECT_DIR}/.lhp_state.json"
    
    if [[ -f "$state_file" ]]; then
        execute_cmd "Deleting .lhp_state.json" "rm -f '$state_file'"
        if [[ "$DRY_RUN" == false ]]; then
            print_success "Deleted .lhp_state.json"
        fi
    else
        print_info ".lhp_state.json does not exist (skipping)"
    fi
}

# Git discard changes in databricks.yml
git_discard_databricks() {
    local databricks_file="tests/e2e/fixtures/testing_project/databricks.yml"
    
    # Change to project root for git operations
    cd "$PROJECT_ROOT" || exit 1
    
    # Check if file has changes
    if git diff --quiet "$databricks_file" 2>/dev/null; then
        print_info "databricks.yml has no changes (skipping)"
    else
        execute_cmd "Discarding changes in databricks.yml" "cd '$PROJECT_ROOT' && git restore '$databricks_file'"
        if [[ "$DRY_RUN" == false ]]; then
            print_success "Discarded changes in databricks.yml"
        fi
    fi
}

# Move generated/dev to generated_baseline/dev
move_generated_to_baseline() {
    if [[ "$MOVE_GENERATED" == false ]]; then
        return 0
    fi
    
    local source_dir="${TESTING_PROJECT_DIR}/generated/dev"
    local target_dir="${TESTING_PROJECT_DIR}/generated_baseline/dev"
    
    if [[ ! -d "$source_dir" ]]; then
        print_warning "Source directory does not exist: generated/dev (skipping)"
        return 0
    fi
    
    print_info "Moving generated/dev to generated_baseline/dev"
    
    # Remove existing target directory if it exists
    if [[ -d "$target_dir" ]]; then
        execute_cmd "  Removing existing generated_baseline/dev" "rm -rf '$target_dir'"
    fi
    
    # Move the directory
    execute_cmd "  Moving generated/dev to generated_baseline/dev" "mv '$source_dir' '$target_dir'"
    
    if [[ "$DRY_RUN" == false ]]; then
        print_success "Moved generated/dev to generated_baseline/dev"
    fi
    return 0
}

# Empty generated directory
empty_generated_dir() {
    if [[ "$EMPTY_GENERATED" == false ]]; then
        return 0
    fi
    
    local generated_dir="${TESTING_PROJECT_DIR}/generated"
    
    if [[ ! -d "$generated_dir" ]]; then
        print_warning "Generated directory does not exist (skipping)"
        return 0
    fi
    
    # Check if directory has contents
    if [[ -z "$(ls -A "$generated_dir")" ]]; then
        print_info "Generated directory is already empty (skipping)"
        return 0
    fi
    
    print_info "Emptying generated directory"
    execute_cmd "  Removing all contents from generated/" "rm -rf '${generated_dir:?}'/*"
    
    if [[ "$DRY_RUN" == false ]]; then
        print_success "Emptied generated directory"
    fi
    return 0
}

# Empty resources directory
empty_resources_dir() {
    if [[ "$EMPTY_RESOURCES" == false ]]; then
        return 0
    fi
    
    local resources_dir="${TESTING_PROJECT_DIR}/resources"
    
    if [[ ! -d "$resources_dir" ]]; then
        print_warning "Resources directory does not exist (skipping)"
        return 0
    fi
    
    # Check if directory has contents
    if [[ -z "$(ls -A "$resources_dir")" ]]; then
        print_info "Resources directory is already empty (skipping)"
        return 0
    fi
    
    print_info "Emptying resources directory"
    execute_cmd "  Removing all contents from resources/" "rm -rf '${resources_dir:?}'/*"
    
    if [[ "$DRY_RUN" == false ]]; then
        print_success "Emptied resources directory"
    fi
    return 0
}

# move resources to resources_baseline
move_resources_to_baseline() {
    if [[ "$MOVE_RESOURCES" == false ]]; then
        return 0
    fi
    
    local source_dir="${TESTING_PROJECT_DIR}/resources"
    local target_dir="${TESTING_PROJECT_DIR}/resources_baseline"
    
    if [[ ! -d "$source_dir" ]]; then
        print_warning "Resources directory does not exist (skipping)"
        return 0
    fi
    
    # Check if source has contents
    if [[ -z "$(ls -A "$source_dir")" ]]; then
        print_info "Resources directory is empty, nothing to move (skipping)"
        return 0
    fi
    
    print_info "Backing up resources to resources_baseline"
    
    # Remove existing target directory contents if it exists
    if [[ -d "$target_dir" ]]; then
        execute_cmd "  Removing existing resources_baseline contents" "rm -rf '${target_dir:?}'/*"
    else
        execute_cmd "  Creating resources_baseline directory" "mkdir -p '$target_dir'"
    fi
    
    # Move contents to baseline
    execute_cmd "  Moving resources/* to resources_baseline/" "mv '${source_dir}'/* '$target_dir/'"
    
    if [[ "$DRY_RUN" == false ]]; then
        print_success "Backed up resources to resources_baseline"
    fi
    return 0
}

# Main execution function
main() {
    local start_time=$(date +%s)
    
    echo ""
    print_info "=========================================="
    print_info "  Testing Project Reset Script"
    print_info "=========================================="
    echo ""
    
    if [[ "$DRY_RUN" == true ]]; then
        print_warning "DRY RUN MODE - No changes will be made"
        echo ""
    fi
    
    # Validate environment
    validate_environment
    echo ""
    
    # Always-run operations
    print_info "Running always-executed operations..."
    delete_lhp_dir
    delete_state_file
    git_discard_databricks
    echo ""
    
    # Optional operations
    if [[ "$MOVE_GENERATED" == true || "$EMPTY_GENERATED" == true || "$EMPTY_RESOURCES" == true || "$MOVE_RESOURCES" == true ]]; then
        print_info "Running optional operations..."
        move_generated_to_baseline
        empty_generated_dir
        empty_resources_dir
        move_resources_to_baseline
        echo ""
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    print_info "=========================================="
    if [[ "$DRY_RUN" == true ]]; then
        print_success "Dry run completed in ${duration}s"
    else
        print_success "Reset completed successfully in ${duration}s"
    fi
    print_info "=========================================="
    echo ""
}

#------------------------------------------------------------------------------
# Argument Parsing
#------------------------------------------------------------------------------

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    NO_ARGS_PASSED=false
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --execute)
            DRY_RUN=false
            shift
            ;;
        --move-generated)
            MOVE_GENERATED=true
            shift
            ;;
        --empty-generated)
            EMPTY_GENERATED=true
            shift
            ;;
        --empty-resources)
            EMPTY_RESOURCES=true
            shift
            ;;
        --move-resources)
            MOVE_RESOURCES=true
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Set defaults if no arguments were passed
if [[ "$NO_ARGS_PASSED" == true ]]; then
    DRY_RUN=true
    EMPTY_RESOURCES=true
    EMPTY_GENERATED=true
fi

#------------------------------------------------------------------------------
# Script Entry Point
#------------------------------------------------------------------------------

main "$@"

