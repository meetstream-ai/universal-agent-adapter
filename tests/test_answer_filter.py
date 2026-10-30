import pytest


def test_crew_answer_boundary_and_split_closing_fence():
    pytest.importorskip("crewai")
    from meetstream_agent_adapter.adapters.crewai import AnswerFilter
    f = AnswerFilter()
    assert not f.feed("Thought: mentions Final Answer: PRIVATE\nAction: save")
    chunks = ["\nThought: PRIVATE\nFinal Ans", "wer: Saved the ", "action.\n`", "``"]
    answer = "".join(f.feed(c) for c in chunks) + f.finish()
    assert answer == "Saved the action."
    assert not f.feed("PRIVATE AFTER FENCE")
