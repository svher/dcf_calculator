/**
 * Editor Component wrapping Monaco Editor
 */

const Editor = {
    instance: null,

    init(containerId) {
        require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs' } });

        // Use a promise to handle async loading if needed, though require is callback based
        return new Promise((resolve, reject) => {
            require(['vs/editor/editor.main'], () => {
                const container = document.getElementById(containerId);
                if (!container) {
                    reject('Container not found');
                    return;
                }

                this.instance = monaco.editor.create(container, {
                    value: '// Select a file to view',
                    language: 'plaintext',
                    theme: 'vs-dark', // Dark theme to match VS Code default
                    automaticLayout: true,
                    minimap: { enabled: true },
                    fontSize: 14,
                    fontFamily: "'Fira Code', Consolas, 'Courier New', monospace",
                    scrollBeyondLastLine: false,
                    readOnly: true // Default to read only until file loaded
                });

                resolve(this.instance);
            });
        });
    },

    setModel(content, language, path) {
        if (!this.instance) return;

        // Try to get existing model for this path (URI) to preserve state if we wanted
        // For now, simpler approach: create/dispose or just setValue

        const model = monaco.editor.createModel(content, language);
        this.instance.setModel(model);
        this.instance.updateOptions({ readOnly: false });
    },

    updateContent(content) {
        if (!this.instance) return;
        this.instance.setValue(content);
    },

    setLanguage(lang) {
        if (!this.instance) return;
        const model = this.instance.getModel();
        monaco.editor.setModelLanguage(model, lang);
    }
};
