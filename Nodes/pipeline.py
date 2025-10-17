"""
Main pipeline orchestrator for the Lendingwise AI system.
"""

from langgraph.graph import END, StateGraph

from .config.state_models import PipelineState
from .nodes.ingestion_node import Ingestion
from .nodes.tamper_check_node import TamperCheck
from .nodes.ocr_node import OCR
from .nodes.classification_node import Classification
from .nodes.extraction_node import Extract
from .nodes.workflow_router import Classified_or_not


def create_pipeline() -> StateGraph:
    """
    Create and configure the main pipeline workflow.
    """
    workflow = StateGraph(PipelineState)

    # Add nodes
    workflow.add_node("Ingestion", Ingestion)
    workflow.add_node("Tamper Check", TamperCheck)
    workflow.add_node("OCR", OCR)
    workflow.add_node("Document Classification", Classification)
    workflow.add_node("Document Data Extraction", Extract)

    # Set entry point
    workflow.set_entry_point("Ingestion")
    
    # Add edges
    workflow.add_edge("Ingestion", "Tamper Check")
    workflow.add_edge("Tamper Check", "OCR")
    workflow.add_edge("OCR", "Document Classification")

    # Add conditional edges
    workflow.add_conditional_edges(
        "Document Classification",
        Classified_or_not,
        {"Document Data Extraction": "Document Data Extraction", "OCR": "OCR"}
    )

    return workflow


def get_compiled_pipeline():
    """
    Get the compiled pipeline ready for execution.
    """
    workflow = create_pipeline()
    return workflow.compile()
