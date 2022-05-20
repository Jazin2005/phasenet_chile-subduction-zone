from PhaseNet_Analysis import run_phasenet

def test_run_phasenet(benchmark):
    
    benchmark(run_phasenet)