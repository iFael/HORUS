"""CLI — Interface de linha de comando do RaioX Público BR."""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from raiox.config import Config
from raiox.database import DatabaseManager
from raiox.analysis import GraphAnalysis
from raiox.graph_builder import GraphBuilder
from raiox.report import ReportGenerator
from raiox.risk_engine import RiskEngine
from raiox.utils import limpar_documento, get_logger

console = Console()
logger = get_logger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="raiox",
        description="🔍 RaioX Público BR — Análise de risco em dados públicos",
    )
    sub = parser.add_subparsers(dest="comando", help="Comandos disponíveis")

    # -- analise ---
    p_analise = sub.add_parser("analise", help="Analisar risco de pessoa/empresa")
    p_analise.add_argument("--cpf", help="CPF da pessoa a analisar")
    p_analise.add_argument("--cnpj", help="CNPJ da empresa a analisar")
    p_analise.add_argument("--nome", help="Nome para busca")

    # -- atualizar ---
    p_atualizar = sub.add_parser("atualizar", help="Atualizar base de dados via ETL")
    p_atualizar.add_argument("--fonte", help="Nome da fonte (transparencia, tse, pncp, ...)")
    p_atualizar.add_argument("--todas", action="store_true", help="Atualizar todas as fontes")
    p_atualizar.add_argument("--cpf", help="CPF para busca direcionada")
    p_atualizar.add_argument("--cnpj", help="CNPJ para busca direcionada")

    # -- exportar ---
    p_exportar = sub.add_parser("exportar", help="Exportar relatório")
    p_exportar.add_argument("--cpf", help="CPF")
    p_exportar.add_argument("--cnpj", help="CNPJ")
    p_exportar.add_argument("--formato", default="html", choices=["markdown", "json", "html"])

    # -- grafo ---
    p_grafo = sub.add_parser("grafo", help="Gerar visualização do grafo")
    p_grafo.add_argument("--cpf", help="CPF")
    p_grafo.add_argument("--cnpj", help="CNPJ")
    p_grafo.add_argument("--profundidade", type=int, default=2)

    # -- scan ---
    p_scan = sub.add_parser("scan", help="Varredura automática de todos os políticos")
    p_scan.add_argument("--rapido", action="store_true", help="Pular coleta de despesas")

    # -- dashboard ---
    sub.add_parser("dashboard", help="Abrir dashboard ORUS no navegador")

    # -- status ---
    sub.add_parser("status", help="Status do banco de dados")

    return parser


def cmd_analise(args: argparse.Namespace) -> None:
    config = Config()
    db = DatabaseManager(config)
    engine = RiskEngine(db, config)

    if args.cpf:
        cpf = limpar_documento(args.cpf)
        console.print(f"\n[bold cyan]Analisando CPF: {cpf}[/bold cyan]\n")
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Calculando risco...", total=None)
            resultado = engine.calcular_risco_cpf(cpf)
            progress.update(task, completed=True)
    elif args.cnpj:
        cnpj = limpar_documento(args.cnpj)
        console.print(f"\n[bold cyan]Analisando CNPJ: {cnpj}[/bold cyan]\n")
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            task = progress.add_task("Calculando risco...", total=None)
            resultado = engine.calcular_risco_cnpj(cnpj)
            progress.update(task, completed=True)
    elif args.nome:
        console.print(f"\n[bold cyan]Buscando: {args.nome}[/bold cyan]\n")
        pessoas = db.buscar_pessoa_nome(args.nome)
        if not pessoas:
            console.print("[yellow]Nenhuma pessoa encontrada.[/yellow]")
            return
        console.print(f"[green]Encontradas {len(pessoas)} pessoas.[/green]\n")
        for p in pessoas:
            resultado = engine.calcular_risco_cpf(p["cpf"])
            _exibir_resultado(resultado)
        return
    else:
        console.print("[red]Informe --cpf, --cnpj ou --nome.[/red]")
        return

    _exibir_resultado(resultado)


def _exibir_resultado(resultado: "raiox.risk_engine.ResultadoRisco") -> None:
    cor = {"Baixo": "green", "Médio": "yellow", "Alto": "red", "Muito Alto": "bold red"}
    nivel_cor = cor.get(resultado.nivel, "white")

    panel = Panel(
        f"[bold]{resultado.nome}[/bold]\n"
        f"CPF/CNPJ: {resultado.cpf_cnpj}\n"
        f"Score: [{nivel_cor}]{resultado.score_total:.1f}/100[/{nivel_cor}]\n"
        f"Nível: [{nivel_cor}]{resultado.nivel}[/{nivel_cor}]",
        title="[cyan]Resultado da Análise[/cyan]",
        border_style="cyan",
    )
    console.print(panel)

    table = Table(title="Indicadores de Risco", show_lines=True)
    table.add_column("Indicador", style="bold")
    table.add_column("Peso", justify="center")
    table.add_column("Score", justify="center")
    table.add_column("Contribuição", justify="center")
    table.add_column("Detalhes")

    for ind in sorted(resultado.indicadores, key=lambda x: -x.contribuicao):
        score_style = "green" if ind.score < 0.3 else "yellow" if ind.score < 0.6 else "red"
        table.add_row(
            ind.nome,
            f"{ind.peso:.0f}",
            f"[{score_style}]{ind.score:.2f}[/{score_style}]",
            f"{ind.contribuicao:.2f}",
            ind.detalhes[:60],
        )

    console.print(table)
    console.print(f"\n[dim]{resultado.resumo}[/dim]\n")


def cmd_atualizar(args: argparse.Namespace) -> None:
    config = Config()
    db = DatabaseManager(config)

    from raiox.etl.transparencia import TransparenciaETL
    from raiox.etl.cgu_sancoes import SancoesETL
    from raiox.etl.pncp import PNCPETL
    from raiox.etl.tse import TSEETL
    from raiox.etl.bcb import BCBETL
    from raiox.etl.ibge import IBGEETL

    etls = {
        "transparencia": TransparenciaETL,
        "sancoes": SancoesETL,
        "pncp": PNCPETL,
        "tse": TSEETL,
        "bcb": BCBETL,
        "ibge": IBGEETL,
    }

    kwargs = {}
    if args.cpf:
        kwargs["cpf"] = args.cpf
    if args.cnpj:
        kwargs["cnpj"] = args.cnpj

    fontes = list(etls.keys()) if args.todas else [args.fonte] if args.fonte else []

    if not fontes:
        console.print("[red]Informe --fonte ou --todas[/red]")
        return

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
        for fonte in fontes:
            cls = etls.get(fonte)
            if not cls:
                console.print(f"[yellow]Fonte '{fonte}' não encontrada. Disponíveis: {list(etls.keys())}[/yellow]")
                continue
            task = progress.add_task(f"ETL: {fonte}...", total=None)
            try:
                etl = cls(db, config)
                count = etl.run(force=True, **kwargs)
                progress.update(task, completed=True)
                console.print(f"  [green]✓ {fonte}: {count} registros[/green]")
            except Exception as e:
                progress.update(task, completed=True)
                console.print(f"  [red]✗ {fonte}: {e}[/red]")


def cmd_exportar(args: argparse.Namespace) -> None:
    config = Config()
    db = DatabaseManager(config)
    engine = RiskEngine(db, config)
    reporter = ReportGenerator(config)

    if args.cpf:
        resultado = engine.calcular_risco_cpf(limpar_documento(args.cpf))
    elif args.cnpj:
        resultado = engine.calcular_risco_cnpj(limpar_documento(args.cnpj))
    else:
        console.print("[red]Informe --cpf ou --cnpj[/red]")
        return

    gb = engine.graph_builder
    analysis = GraphAnalysis(gb)
    path = reporter.generate(resultado, analysis, formato=args.formato)
    console.print(f"[green]Relatório exportado: {path}[/green]")


def cmd_grafo(args: argparse.Namespace) -> None:
    config = Config()
    db = DatabaseManager(config)
    gb = GraphBuilder(db, config)

    if args.cpf:
        gb.build_from_cpf(limpar_documento(args.cpf), profundidade=args.profundidade)
    elif args.cnpj:
        gb.build_from_cnpj(limpar_documento(args.cnpj), profundidade=args.profundidade)
    else:
        console.print("[red]Informe --cpf ou --cnpj[/red]")
        return

    gb.add_same_address_edges()
    gb.add_family_edges()

    output = str(config.paths.exports / "grafo.html")
    gb.to_pyvis_html(output)
    console.print(f"[green]Grafo gerado: {output}[/green]")

    analysis = GraphAnalysis(gb)
    metrics = gb.metrics()
    console.print(f"Nós: {metrics['nodes']}, Arestas: {metrics['edges']}")


def cmd_scan(args: argparse.Namespace) -> None:
    """Executa varredura automática de todos os deputados e senadores."""
    config = Config()
    db = DatabaseManager(config)

    from raiox.scanner import PoliticianScanner
    scanner = PoliticianScanner(db)

    skip = getattr(args, "rapido", False)

    def on_progress(etapa, detalhe=""):
        console.print(f"  [bold orange1]{etapa}[/bold orange1] {detalhe}")

    console.print(Panel(
        "[bold orange1]⚡ ORUS — Scanner de Políticos[/bold orange1]\n"
        "[dim]Rastreando deputados e senadores automaticamente...[/dim]",
        border_style="orange1",
    ))

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
        task = progress.add_task("Executando varredura...", total=None)
        result = scanner.scan_all(
            skip_despesas=skip,
            progress_callback=on_progress,
        )
        progress.update(task, completed=True)

    if result.get("status") == "concluido":
        etapas = result.get("etapas", {})
        dur = result.get("duracao_s", 0)
        console.print(f"\n[green]✓ Varredura concluída em {dur:.0f}s[/green]")
        console.print(f"  Políticos: {etapas.get('politicos', 0)}")
        console.print(f"  Insights: {etapas.get('insights', 0)}")
        console.print(f"  Alertas: {etapas.get('alertas', 0)}")
    else:
        console.print(f"[red]✗ Erro: {result.get('erro', 'desconhecido')}[/red]")


def cmd_dashboard(args: argparse.Namespace) -> None:
    """Abre o dashboard ORUS."""
    import subprocess
    import sys as _sys
    console.print("[bold orange1]Abrindo dashboard ORUS...[/bold orange1]")
    from pathlib import Path as _Path
    web_path = _Path(__file__).parent / "web.py"
    subprocess.Popen([_sys.executable, "-m", "streamlit", "run", str(web_path), "--server.headless", "true"])
    console.print("[green]Dashboard disponível em http://localhost:8501[/green]")


def cmd_status(args: argparse.Namespace) -> None:
    config = Config()
    db = DatabaseManager(config)
    stats = db.estatisticas()

    table = Table(title="Status do Banco de Dados", show_lines=True)
    table.add_column("Tabela", style="bold")
    table.add_column("Registros", justify="right")

    total = 0
    for tabela, count in sorted(stats.items()):
        table.add_row(tabela, str(count))
        total += count

    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
    console.print(table)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    console.print(Panel(
        "[bold cyan]🔍 RaioX Público BR[/bold cyan]\n"
        "[dim]Sistema de Análise de Risco em Dados Públicos[/dim]",
        border_style="cyan",
    ))

    commands = {
        "analise": cmd_analise,
        "atualizar": cmd_atualizar,
        "exportar": cmd_exportar,
        "grafo": cmd_grafo,
        "scan": cmd_scan,
        "dashboard": cmd_dashboard,
        "status": cmd_status,
    }

    if args.comando in commands:
        try:
            commands[args.comando](args)
        except KeyboardInterrupt:
            console.print("\n[yellow]Operação cancelada.[/yellow]")
        except Exception as e:
            console.print(f"[red]Erro: {e}[/red]")
            logger.exception("Erro na execução")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
