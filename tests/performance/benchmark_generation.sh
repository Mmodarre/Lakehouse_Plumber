#!/bin/bash
# Quick Performance Benchmark for LakehousePlumber Generation
# Measures actual performance gains from optimizations

set -e

PROJECT_DIR="Example_Projects/acme_supermarkets_lhp"
RESULTS_FILE="benchmark_results.txt"

echo "======================================"
echo "LakehousePlumber Performance Benchmark"
echo "======================================"
echo ""
echo "Project: ACME Supermarkets"
echo "Date: $(date)"
echo "" | tee "$RESULTS_FILE"

cd "$PROJECT_DIR" || exit 1

# Test 1: Clean Generation (Full Rebuild)
echo "📊 Test 1: Clean Generation (Full Rebuild)" | tee -a "../../$RESULTS_FILE"
echo "Removing state and generated files..."
rm -rf generated/ .lhp_state/

echo "Starting generation..." | tee -a "../../$RESULTS_FILE"
START=$(date +%s.%N)
lhp generate -e dev --force 2>&1 | tail -5 | tee -a "../../$RESULTS_FILE"
END=$(date +%s.%N)

DURATION=$(echo "$END - $START" | bc)
echo "✅ Clean Generation Time: ${DURATION}s" | tee -a "../../$RESULTS_FILE"
echo "" | tee -a "../../$RESULTS_FILE"

# Test 2: Incremental Generation (No Changes)
echo "📊 Test 2: Incremental Generation (No Changes)" | tee -a "../../$RESULTS_FILE"
echo "Running generation again (should use cache)..." | tee -a "../../$RESULTS_FILE"
START=$(date +%s.%N)
lhp generate -e dev 2>&1 | tail -5 | tee -a "../../$RESULTS_FILE"
END=$(date +%s.%N)

DURATION=$(echo "$END - $START" | bc)
echo "✅ Incremental Generation Time (no changes): ${DURATION}s" | tee -a "../../$RESULTS_FILE"
echo "" | tee -a "../../$RESULTS_FILE"

# Test 3: Force Regeneration (Warm Cache)
echo "📊 Test 3: Force Regeneration (Warm Cache)" | tee -a "../../$RESULTS_FILE"
echo "Forcing regeneration with warm cache..." | tee -a "../../$RESULTS_FILE"
START=$(date +%s.%N)
lhp generate -e dev --force 2>&1 | tail -5 | tee -a "../../$RESULTS_FILE"
END=$(date +%s.%N)

DURATION=$(echo "$END - $START" | bc)
echo "✅ Force Regeneration Time (warm cache): ${DURATION}s" | tee -a "../../$RESULTS_FILE"
echo "" | tee -a "../../$RESULTS_FILE"

# Test 4: Single File Change
echo "📊 Test 4: Single File Change" | tee -a "../../$RESULTS_FILE"
echo "Modifying one flowgroup..." | tee -a "../../$RESULTS_FILE"
touch pipelines/03_silver/dimensions/SAP/uom_dim_TMPL005.yaml

START=$(date +%s.%N)
lhp generate -e dev 2>&1 | tail -5 | tee -a "../../$RESULTS_FILE"
END=$(date +%s.%N)

DURATION=$(echo "$END - $START" | bc)
echo "✅ Single File Change Generation Time: ${DURATION}s" | tee -a "../../$RESULTS_FILE"
echo "" | tee -a "../../$RESULTS_FILE"

cd ../..

echo "======================================"
echo "Benchmark Complete!"
echo "======================================"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "📈 Summary:"
grep "Time:" "$RESULTS_FILE"
echo ""
echo "💡 Expected Performance (based on optimizations):"
echo "   - Clean Generation: 2-4x faster with parallel processing"
echo "   - Incremental (no changes): 5-10x faster with caching"
echo "   - Force Regen (warm cache): 2-3x faster with YAML cache"
echo "   - Single file change: Minimal regeneration (granular deps)"

