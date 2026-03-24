SYSTEM_PROMPT = """
You are a research assistant specializing in systems and machine learning.
"""

ASSESS_PROMPT = """
You are evaluating whether a paper is worth reading for a research digest focused on:
- general operating systems
- general AI infrastructure / ML systems
- AI compilers
- compiler design and implementation
- program analysis

Your task:
1. Decide whether the paper is relevant to this digest.
2. If relevant, score how worth-reading it is on a 0-100 scale.
3. Use the abstract as the primary signal.
4. Use author affiliations as a secondary signal only:
   - strong research labs, systems groups, and major industry infra teams can increase confidence
   - missing or unknown affiliations should not heavily penalize the paper
   - affiliations must never override a weak abstract

Treat as relevant when the paper's main contribution is primarily in one of these buckets:
- OS: scheduling, storage, file systems, memory systems, virtualization, networking, distributed systems, resource management, performance isolation, systems implementation
- AI-Infra: training/inference systems, serving systems, ML runtime, cluster orchestration, memory/communication/storage/caching/checkpointing for AI workloads, parallel execution and resource management for ML
- AI-Compiler: graph compilers, MLIR/TensorIR/XLA-style compilation, lowering, code generation, scheduling, fusion, kernel generation, auto-tuning tightly coupled to compilation, compiler/runtime co-design for AI execution
- Compiler: compiler architecture, compiler passes, optimization pipelines, intermediate representations, JIT/AOT compilation, language implementation, optimization design and implementation
- Program-Analysis: static analysis, dynamic analysis, abstract interpretation, dataflow analysis, alias/points-to analysis, bug finding, performance analysis, compiler analyses, debugging/profiling analyses

Treat as not relevant when the paper is primarily about:
- federated learning
- IoT or edge applications where the core novelty is the application
- edge deployment, embedded inference, DVFS, power optimization, or hardware-centric optimization for edge devices
- privacy, differential privacy, secure computation, compliance, or governance
- AI for science or vertical-domain applications
- pure model architecture, datasets, benchmarks, or algorithmic improvements without a strong compiler or program-analysis contribution
- recommendation, robotics, agents, multimodal products, or application features
- hardware accelerator design without a compiler/program-analysis core contribution
- pure formal methods or verification papers unless the contribution strongly centers on practical compiler/program-analysis techniques

In particular, return NOT relevant for papers centered on:
- power-aware or energy-aware edge inference
- hardware edge accelerators
- DVFS-based DNN deployment on edge devices
- embedded device optimization where the main novelty is hardware/power management rather than AI compilation / compiler implementation / program analysis
- pure FPGA/ASIC hardware papers where the main contribution is hardware architecture rather than systems, compiler, or analysis

Preference rules:
- OS, AI-Infra, AI-Compiler, Compiler, and Program-Analysis are all valid areas for this digest.
- Do NOT give a higher score merely because a paper is in AI-Compiler, Compiler, or Program-Analysis.
- Judge quality first: novelty, technical depth, likely impact, clarity of contribution, and whether it seems worth a serious read.
- A strong OS or AI-Infra paper should outrank a mediocre compiler/program-analysis paper.
- A strong compiler/program-analysis paper should outrank a mediocre OS or AI-Infra paper.
- Papers about compilers for GPUs/TPUs/FPGAs/ASICs are relevant if the main contribution is the compiler, IR, lowering, scheduling, code generation, or analysis.
- Papers about FPGA/ASIC/edge hardware are not relevant if the main contribution is hardware design, deployment engineering, or power/energy optimization.

Scoring guidance for relevant papers:
- 90-100: strong must-read, likely impactful, technically deep, and genuinely worth prioritizing
- 75-89: clearly worth reading, strong contribution and good fit
- 60-74: maybe worth reading, decent fit but narrower or less compelling
- below 60: relevant but low priority

Default to NOT relevant when uncertain.

Formatting rules:
- If relevant is false, set score to 0 and fit_area to "Irrelevant".
- fit_area must be one of: "OS", "AI-Infra", "AI-Compiler", "Compiler", "Program-Analysis", "Mixed", "Irrelevant".
- affiliation_signal must always be a non-empty string. If there is no useful signal, say so explicitly.

Return ONLY valid JSON:
{
  "relevant": true,
  "score": 84,
  "fit_area": "AI-Compiler",
  "reason": "Why it is or is not worth reading for this digest.",
  "affiliation_signal": "How the author affiliations affect confidence, or say that no useful affiliation signal is available."
}
"""

SUMMARY_PROMPT = """
Given a paper, do:

1. Summarize in 3 bullet points
2. Explain the key idea simply
3. Translate into Chinese

Return JSON:
{
  "summary": ["...", "...", "..."],
  "explanation": "...",
  "translation": "..."
}

Return ONLY valid JSON. Do not use markdown code fences.
"""
