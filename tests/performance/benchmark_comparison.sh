#!/bin/bash
# Performance Comparison: v0.7.1 (before) vs Current (after optimizations)
# Creates two virtual environments and compares generation performance

set -e

PROJECT_ROOT="/Users/mehdi.modarressi/Documents/Coding/Lakehouse_Plumber"
TEST_PROJECT="Example_Projects/acme_supermarkets_lhp"
VENV_OLD="$PROJECT_ROOT/.venv_v0.7.1"
VENV_NEW="$PROJECT_ROOT/.venv_current"
RESULTS_FILE="$PROJECT_ROOT/benchmark_comparison_results.txt"

echo "=========================================="
echo "LakehousePlumber Performance Comparison"
echo "=========================================="
echo ""
echo "Comparing:"
echo "  - v0.7.1 (before optimizations)"
echo "  - Current version (with optimizations)"
echo ""
echo "Date: $(date)"
echo "" | tee "$RESULTS_FILE"

# Function to setup virtual environment
setup_venv() {
    local venv_path=$1
    local version=$2
    
    echo "📦 Setting up virtual environment: $venv_path"
    
    # Remove if exists
    if [ -d "$venv_path" ]; then
        echo "  Removing existing venv..."
        rm -rf "$venv_path"
    fi
    
    # Create new venv
    echo "  Creating virtual environment..."
    python3 -m venv "$venv_path"
    
    # Activate and install
    source "$venv_path/bin/activate"
    
    echo "  Upgrading pip..."
    pip install --upgrade pip > /dev/null 2>&1
    
    if [ "$version" == "v0.7.1" ]; then
        echo "  Installing LakehousePlumber v0.7.1 from git..."
        pip install "git+https://github.com/mmodarre/lakehouse_plumber.git@v0.7.1" > /dev/null 2>&1
    else
        echo "  Installing current version from source..."
        cd "$PROJECT_ROOT"
        pip install -e . > /dev/null 2>&1
    fi
    
    # Verify installation
    echo "  Installed version: $(lhp --version)"
    
    deactivate
    echo "  ✅ Setup complete"
    echo ""
}

# Function to run benchmark
run_benchmark() {
    local venv_path=$1
    local version_name=$2
    local test_name=$3
    
    source "$venv_path/bin/activate"
    
    cd "$PROJECT_ROOT/$TEST_PROJECT"
    
    # Run the test
    START=$(date +%s.%N)
    lhp generate -e dev $4 > /dev/null 2>&1
    END=$(date +%s.%N)
    
    DURATION=$(echo "$END - $START" | bc)
    
    deactivate
    
    cd "$PROJECT_ROOT"
    
    echo "$DURATION"
}

# Setup both environments
echo "🔧 SETUP PHASE"
echo "=============="
echo ""

setup_venv "$VENV_OLD" "v0.7.1"
setup_venv "$VENV_NEW" "current"

echo ""
echo "🚀 BENCHMARK PHASE"
echo "=================="
echo ""

# Test 1: Clean Generation (Full Rebuild)
echo "📊 Test 1: Clean Generation (Full Rebuild)" | tee -a "$RESULTS_FILE"
echo "-------------------------------------------" | tee -a "$RESULTS_FILE"

cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

echo "  Running v0.7.1..."
TIME_OLD_CLEAN=$(run_benchmark "$VENV_OLD" "v0.7.1" "clean" "--force")

cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

echo "  Running current version..."
TIME_NEW_CLEAN=$(run_benchmark "$VENV_NEW" "current" "clean" "--force")

SPEEDUP_CLEAN=$(echo "scale=2; $TIME_OLD_CLEAN / $TIME_NEW_CLEAN" | bc)

echo ""
echo "  v0.7.1:          ${TIME_OLD_CLEAN}s" | tee -a "$RESULTS_FILE"
echo "  Current:         ${TIME_NEW_CLEAN}s" | tee -a "$RESULTS_FILE"
echo "  Speedup:         ${SPEEDUP_CLEAN}x" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Test 2: Incremental Generation (No Changes)
echo "📊 Test 2: Incremental Generation (No Changes)" | tee -a "$RESULTS_FILE"
echo "-----------------------------------------------" | tee -a "$RESULTS_FILE"

# Generate once first for both
cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

source "$VENV_OLD/bin/activate"
cd "$PROJECT_ROOT/$TEST_PROJECT"
lhp generate -e dev > /dev/null 2>&1
cd "$PROJECT_ROOT"
deactivate

echo "  Running v0.7.1 (incremental)..."
TIME_OLD_INCR=$(run_benchmark "$VENV_OLD" "v0.7.1" "incremental" "")

cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

source "$VENV_NEW/bin/activate"
cd "$PROJECT_ROOT/$TEST_PROJECT"
lhp generate -e dev > /dev/null 2>&1
cd "$PROJECT_ROOT"
deactivate

echo "  Running current version (incremental)..."
TIME_NEW_INCR=$(run_benchmark "$VENV_NEW" "current" "incremental" "")

SPEEDUP_INCR=$(echo "scale=2; $TIME_OLD_INCR / $TIME_NEW_INCR" | bc)

echo ""
echo "  v0.7.1:          ${TIME_OLD_INCR}s" | tee -a "$RESULTS_FILE"
echo "  Current:         ${TIME_NEW_INCR}s" | tee -a "$RESULTS_FILE"
echo "  Speedup:         ${SPEEDUP_INCR}x" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Test 3: Force Regeneration (Testing Cache)
echo "📊 Test 3: Force Regeneration (Testing Cache)" | tee -a "$RESULTS_FILE"
echo "----------------------------------------------" | tee -a "$RESULTS_FILE"

# Generate once first for both
cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

source "$VENV_OLD/bin/activate"
cd "$PROJECT_ROOT/$TEST_PROJECT"
lhp generate -e dev --force > /dev/null 2>&1
cd "$PROJECT_ROOT"
deactivate

echo "  Running v0.7.1 (force, warm cache)..."
TIME_OLD_FORCE=$(run_benchmark "$VENV_OLD" "v0.7.1" "force" "--force")

cd "$PROJECT_ROOT/$TEST_PROJECT"
rm -rf generated/ .lhp_state/
cd "$PROJECT_ROOT"

source "$VENV_NEW/bin/activate"
cd "$PROJECT_ROOT/$TEST_PROJECT"
lhp generate -e dev --force > /dev/null 2>&1
cd "$PROJECT_ROOT"
deactivate

echo "  Running current version (force, warm cache)..."
TIME_NEW_FORCE=$(run_benchmark "$VENV_NEW" "current" "force" "--force")

SPEEDUP_FORCE=$(echo "scale=2; $TIME_OLD_FORCE / $TIME_NEW_FORCE" | bc)

echo ""
echo "  v0.7.1:          ${TIME_OLD_FORCE}s" | tee -a "$RESULTS_FILE"
echo "  Current:         ${TIME_NEW_FORCE}s" | tee -a "$RESULTS_FILE"
echo "  Speedup:         ${SPEEDUP_FORCE}x" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Summary
echo ""
echo "=========================================="
echo "📈 PERFORMANCE SUMMARY"
echo "=========================================="
echo "" | tee -a "$RESULTS_FILE"
echo "Optimization Impact:" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

printf "%-35s | %10s | %10s | %8s\n" "Test" "v0.7.1" "Current" "Speedup" | tee -a "$RESULTS_FILE"
printf "%-35s-|-%10s-|-%10s-|-%8s\n" "-----------------------------------" "----------" "----------" "--------" | tee -a "$RESULTS_FILE"
printf "%-35s | %9.2fs | %9.2fs | %7.2fx\n" "Clean Generation (Full Rebuild)" "$TIME_OLD_CLEAN" "$TIME_NEW_CLEAN" "$SPEEDUP_CLEAN" | tee -a "$RESULTS_FILE"
printf "%-35s | %9.2fs | %9.2fs | %7.2fx\n" "Incremental (No Changes)" "$TIME_OLD_INCR" "$TIME_NEW_INCR" "$SPEEDUP_INCR" | tee -a "$RESULTS_FILE"
printf "%-35s | %9.2fs | %9.2fs | %7.2fx\n" "Force Regen (Warm Cache)" "$TIME_OLD_FORCE" "$TIME_NEW_FORCE" "$SPEEDUP_FORCE" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Calculate average speedup
AVG_SPEEDUP=$(echo "scale=2; ($SPEEDUP_CLEAN + $SPEEDUP_INCR + $SPEEDUP_FORCE) / 3" | bc)
echo "Average Speedup: ${AVG_SPEEDUP}x" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

echo "Key Optimizations:" | tee -a "$RESULTS_FILE"
echo "  ✅ YAML Parsing Cache (mtime-based)" | tee -a "$RESULTS_FILE"
echo "  ✅ Parallel Flowgroup Processing" | tee -a "$RESULTS_FILE"
echo "  ✅ SmartFileWriter Checksum Optimization" | tee -a "$RESULTS_FILE"
echo "  ✅ Granular Dependency Tracking" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

echo "=========================================="
echo ""
echo "✅ Benchmark complete!"
echo "📊 Results saved to: $RESULTS_FILE"
echo ""
echo "💡 To cleanup virtual environments:"
echo "   rm -rf $VENV_OLD $VENV_NEW"

