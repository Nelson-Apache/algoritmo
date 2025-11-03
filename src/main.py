"""
Launcher principal de la aplicación GUI.

Este archivo inicia la interfaz de usuario basada en pywebview definida en
`academic_analysis_gui.py`. El antiguo CLI quedó obsoleto y causaba
errores de importación, por lo que se simplificó este entrypoint.
"""

from academic_analysis_gui import main as gui_main


def main():
    gui_main()


if __name__ == '__main__':
    main()
