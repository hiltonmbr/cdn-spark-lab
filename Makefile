.DEFAULT_GOAL := help
SCALE ?= small
ALL_PROFILES := --profile cluster --profile s3 --profile hadoop

.PHONY: help generate-data spark s3 hadoop down clean clean-data \
        status logs shell-% setup-env jupyter strip check lint test

help:
	@echo "⚡✨ Bem-vindo ao Spark Lab! De local[*] ao YARN+HDFS e S3! ✨⚡"
	@echo "🚀 Escolha sua aventura abaixo:"
	@echo ""
	@echo "  🧬 make generate-data SCALE=small|large - 🏭 Gera o dataset sintético de vendas/clientes/categorias"
	@echo ""
	@echo "  🔥 make spark       - 🏗️  Inicia o Spark Cluster (master + 2 workers + Spark Connect) — Caso B"
	@echo "  🪣 make s3            - 🏗️  Inicia o cluster + RustFS (4 drives, Erasure Coding) — Caso D"
	@echo "  🐘 make hadoop        - 🏗️  Inicia HDFS + YARN (2 DataNodes, 2 NodeManagers) — Caso C"
	@echo "  🛑 make down             - 😴 Para tudo (qualquer profile)"
	@echo "  💣 make clean            - ☢️  Destrói containers, volumes E dados locais/HDFS"
	@echo "  🧹 make clean-data       - 🗑️  Limpa apenas datasets gerados em data/ e temp/"
	@echo "  📡 make status           - 🔍 Mostra containers em execução (qualquer profile)"
	@echo "  📜 make logs             - 📋 Exibe logs de todos os containers em execução"
	@echo "  🐚 make shell-<name>     - 👨‍💻 Abre um shell em qualquer container (spark-master, namenode, datanode1, ...)"
	@echo ""
	@echo "  🐍 make setup-env        - 🪄  Cria o Python venv e instala dependências com uv"
	@echo "  📓 make jupyter          - 🚀 Inicia o Jupyter Lab (mesmo comando para os 4 casos)"
	@echo "  🧹 make strip            - ✂️  Remove todas as saídas dos notebooks"
	@echo "  🔍 make check            - 🧪 Verifica se os notebooks estão sem saídas (seguro para CI)"
	@echo "  🎨 make lint             - 🐍 Executa o ruff linter em scripts/ e notebooks/"
	@echo "  🧪 make test             - 🔬 Executa pytest + nbmake para testes de ponta a ponta nos notebooks"
	@echo ""

generate-data:
	@echo "🏭 Gerando dataset sintético (scale=$(SCALE))..."
	uv run python scripts/generate_dataset.py --scale $(SCALE)

spark:
	@echo "🔥⚡ Iniciando cluster Spark (master + 2 workers + Spark Connect)... 🚀"
	docker compose --profile cluster up -d
	@echo ""
	@echo "🎉 Cluster está no ar!"
	@echo "   🖥️  Master UI:       http://localhost:8080"
	@echo "   🖥️  Worker UIs:      http://localhost:8081  http://localhost:8082"
	@echo "   🔌 Spark Connect:   sc://localhost:15002"
	@echo "   📊 Spark App UI:    http://localhost:4040"

s3:
	@echo "🔥⚡ Iniciando cluster Spark Standalone + RustFS (4 drives, Erasure Coding)... 🚀"
	docker compose --profile cluster --profile s3 up -d
	@echo ""
	@echo "🎉 Cluster + Object Storage estão no ar!"
	@echo "   🔌 Spark Connect:   sc://localhost:15002"
	@echo "   🌐 S3 API:          http://localhost:9000"
	@echo "   🖥️  RustFS Console:  http://localhost:9001  (admin / adminpassword)"

hadoop:
	@echo "🐘✨ Iniciando HDFS + YARN (2 DataNodes, 2 NodeManagers)... 🚀"
	docker compose --profile hadoop up -d
	@echo ""
	@echo "🎉 Cluster Hadoop está no ar!"
	@echo "   🧠 NameNode UI:         http://localhost:9870"
	@echo "   🚦 ResourceManager UI:  http://localhost:8088"
	@echo "   🌉 HttpFS gateway:      http://localhost:14000"
	@echo "   💡 O Spark driver executa no seu HOST neste caso também — veja docs/07."

down:
	@echo "😴 Parando tudo... 🌙"
	docker compose $(ALL_PROFILES) down

clean:
	@echo "💥☢️  DETONANDO tudo! Containers, volumes e dados locais/HDFS! 💀"
	docker compose $(ALL_PROFILES) down -v
	rm -rf config/hadoop/*.xml config/hadoop/*.xml.raw config/hadoop/exclude-hosts
	rm -rf temp/*
	@echo "🧼 Tudo limpo!"

clean-data:
	@echo "🧹 Varrendo datasets gerados de data/ e temp/..."
	rm -rf data/* temp/*
	touch data/.gitkeep
	@echo "✅ data/ e temp/ estão limpos."

status:
	@echo "📡 Status dos containers:"
	docker compose $(ALL_PROFILES) ps

logs:
	docker compose $(ALL_PROFILES) logs -f

shell-%:
	@echo "👨‍💻 Abrindo shell em $*..."
	docker exec -it $* /bin/bash

setup-env:
	@echo "🐍⚡ Criando ambiente Python com uv..."
	uv venv --clear
	uv sync
	uv run python -m ipykernel install --user --name=cdn-spark-lab --display-name="Python (cdn-spark-lab)"
	git config filter.nbstripout.smudge cat
	git config filter.nbstripout.clean "uv run python3 -c \"import sys,json; nb=json.load(sys.stdin); [c.update({'outputs':[],'execution_count':None}) for c in nb['cells'] if c.get('cell_type')=='code']; json.dump(nb, sys.stdout, indent=1, ensure_ascii=False)\""
	@echo "✅ Ambiente pronto! Ative com: source .venv/bin/activate"
	@echo "✅ Kernel Jupyter 'cdn-spark-lab' registrado — selecione-o nos notebooks (não use o genérico 'Python 3')."

jupyter:
	@echo "📓🚀 Iniciando Jupyter Lab..."
	uv run jupyter lab --notebook-dir=notebooks

strip:
	@echo "🧹 Removendo saídas dos notebooks..."
	@for f in notebooks/*.ipynb; do \
		uv run python3 -c "import json,sys; f=sys.argv[1]; nb=json.load(open(f)); [c.update(outputs=[], execution_count=None) for c in nb['cells'] if c.get('cell_type')=='code']; json.dump(nb, open(f,'w'), indent=1, ensure_ascii=False); print('  ✓ '+f)" "$$f"; \
	done

check:
	@echo "🔍 Verificando se todos os notebooks estão sem saídas..."
	@uv run python3 -c "\
import json, glob, sys; \
failures = []; \
[failures.extend([(f, c.get('id','?')) for c in json.load(open(f))['cells'] if c.get('cell_type')=='code' and (c.get('outputs') or c.get('execution_count') is not None)]) for f in glob.glob('notebooks/*.ipynb')]; \
[print(f'  ❌ {f} célula {c} tem saídas — execute make strip') for f, c in failures] or print('  ✅ Todos os notebooks estão limpos'); \
sys.exit(1 if failures else 0)"

lint:
	@echo "🎨 Executando ruff linter..."
	uv run ruff check scripts/ notebooks/

test:
	@echo "🧪 Executando testes dos notebooks (o profile docker relevante deve estar em execução)..."
	uv run pytest tests/ --nbmake --nbmake-timeout=600 -v
