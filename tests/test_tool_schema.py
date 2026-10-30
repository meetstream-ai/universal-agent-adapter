import pytest
from pydantic import ValidationError

from meetstream_agent_adapter.adapters.common import args_model


def test_crewai_schema_preserves_omission_and_rejects_silent_coercion():
    schema = {'type': 'object', 'properties': {'title': {'type': 'string'}, 'count': {'type': 'integer'}},
              'required': ['title'], 'additionalProperties': False}
    model = args_model('ToolArguments', schema)
    assert model.model_validate({'title': 'Test'}).model_dump() == {'title': 'Test'}
    for invalid in [{'title': 'Test', 'scope': 'other'}, {'title': 4}, {'title': 'Test', 'count': '4'}]:
        with pytest.raises(ValidationError):
            model.model_validate(invalid)
