# ML Service

This microservice provides AI-enhanced skill extraction for the main application.

## Run

```bash
cd /home/quasar/repos/dynamic-skill-gap-radar
source .venv/bin/activate
pip install -r ml_service/requirements.txt
uvicorn ml_service.app:app --host 0.0.0.0 --port 8100 --reload
```

## Endpoints

- `POST /extract_skills`
  - body: `{ "text": "..." }`
  - response: `{ "skills": [{"name": "Python", "score": 0.95}], "model_version": "..." }`

- `POST /normalize_skills`
  - body: `{ "skills": ["PyTorch", "ML", "React.js"] }`
  - response: `{ "normalized": ["Pytorch", "Machine Learning", "React"] }`

## Notes

This initial version uses a lightweight zero-shot similarity approach against the project skill taxonomy.
You can later replace internals with a fine-tuned NER model without changing API contracts.
