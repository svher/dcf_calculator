/**
 * Main Application Logic
 */

const App = {
    init() {
        console.log('Initializing App...');

        // Initialize Components
        this.initResizer();
        // Mermaid init is deferred until after state load to prevent race conditions
        // this.initMermaid();

        // Initialize File Tree and Editor
        FileTree.init('file-tree', this.handleFileSelect.bind(this));

        // Initialize Sidebar Toggle (Horizontal)
        // Initialize Sidebar Toggle (Horizontal)
        const sidebarToggle = document.getElementById('sidebar-toggle');
        const sidebar = document.getElementById('vscode-sidebar');

        if (sidebarToggle && sidebar) {
            // Restore saved state
            const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
            if (isCollapsed) {
                sidebar.classList.add('sidebar-collapsed');
            }

            sidebarToggle.addEventListener('click', () => {
                sidebar.classList.toggle('sidebar-collapsed');

                // Save state
                const currentCollapsed = sidebar.classList.contains('sidebar-collapsed');
                localStorage.setItem('sidebarCollapsed', currentCollapsed);

                // Trigger resize for Monaco
                if (Editor.instance) Editor.instance.layout();
            });
        }

        // Capture initial state synchronously to avoid race conditions with graph auto-loading
        this.initialState = localStorage.getItem('appState');

        Editor.init('monaco-container').then(async () => {
            console.log('Monaco Editor ready');
            window.addEventListener('resize', () => {
                // Debounce resize
                if (this.resizeTimeout) clearTimeout(this.resizeTimeout);
                this.resizeTimeout = setTimeout(() => {
                    Editor.instance.layout();
                }, 50);
            });

            // Restore App State (Tabs) after editor is ready, using captured state
            // Await full restoration before initializing Mermaid to prevents tabs override
            await this.loadState(this.initialState);

            // Now safe to init Mermaid
            this.initMermaid();
        });
    },

    // --- Resizer Logic from Original ---
    initResizer() {
        const resizer = document.getElementById('drag-handle');
        const leftPane = document.getElementById('left-pane');
        const rightPane = document.getElementById('right-pane');

        if (!resizer || !leftPane || !rightPane) return;

        // Restore saved ratio
        try {
            const savedRatio = localStorage.getItem('splitPaneRatio');
            if (savedRatio) {
                const ratio = parseFloat(savedRatio);
                if (!isNaN(ratio) && ratio > 20 && ratio < 80) {
                    leftPane.style.width = ratio + '%';
                    rightPane.style.width = (100 - ratio) + '%';
                }
            }
        } catch (e) {
            console.warn('Failed to restore split layout:', e);
        }

        let isResizing = false;

        resizer.addEventListener('mousedown', (e) => {
            isResizing = true;
            document.body.classList.add('resizing');
            resizer.classList.add('resizing');
        });

        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;
            const containerWidth = document.getElementById('main-split-view').offsetWidth;
            const newLeftWidth = (e.clientX / containerWidth) * 100;

            if (newLeftWidth > 20 && newLeftWidth < 80) {
                leftPane.style.width = `${newLeftWidth}%`;
                rightPane.style.width = `${100 - newLeftWidth}%`;

                // Force layout update for Monaco
                if (Editor.instance) Editor.instance.layout();
            }
        });

        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                document.body.classList.remove('resizing');
                resizer.classList.remove('resizing');

                // Save preference
                const currentRatio = parseFloat(leftPane.style.width);
                localStorage.setItem('splitPaneRatio', currentRatio);
            }
        });
    },

    // --- Mermaid Logic ---
    initMermaid() {
        mermaid.initialize({ startOnLoad: false, theme: 'base', securityLevel: 'loose' });

        // Restore last graph if available, otherwise main
        const lastGraph = localStorage.getItem('currentGraphId') || 'main';
        this.renderGraph(lastGraph);

        // Back button logic - refined to make the whole container clickable
        const headerContainer = document.getElementById('graph-header-container');
        const backBtn = document.getElementById('back-btn');

        headerContainer.addEventListener('click', () => {
            // Only go back if we are NOT on the main graph (i.e., backBtn is visible)
            if (!backBtn.classList.contains('hidden')) {
                this.renderGraph('main');
                document.getElementById('graph-title').textContent = 'Dependency Graph';
                backBtn.classList.add('hidden');

                // Reset container styles
                headerContainer.classList.remove('cursor-pointer', 'hover:bg-slate-100');
            }
        });
    },

    renderGraph(graphId) {
        const container = document.getElementById('graph-container');
        const template = document.getElementById(`graph-${graphId}`);
        if (!template) return;

        // Save state
        localStorage.setItem('currentGraphId', graphId);

        const graphDef = template.textContent.trim();
        container.innerHTML = `<div class="mermaid-output w-full h-full flex items-center justify-center"></div>`;

        mermaid.render(`mermaid-${Date.now()}`, graphDef).then(result => {
            container.querySelector('.mermaid-output').innerHTML = result.svg;

            if (graphId === 'main') {
                this.bindMainGraphInteractivity();
                // Hide back button if on main
                const backBtn = document.getElementById('back-btn');
                if (backBtn) backBtn.classList.add('hidden');
            } else {
                this.bindDetailGraphInteractivity(graphId);
                // Show back button
                const backBtn = document.getElementById('back-btn');
                const title = document.getElementById('graph-title');
                if (backBtn) backBtn.classList.remove('hidden');
                if (title) title.textContent = graphId;
            }
        });
    },

    bindMainGraphInteractivity() {
        // Logic to drill down into subgraphs
        const nodes = document.querySelectorAll('#graph-container g.node');
        nodes.forEach(node => {
            // Check if this node maps to a detail graph
            const nodeId = node.id;
            let logicalId = this.extractLogicalId(nodeId);

            // Check if we have a template for this ID
            if (document.getElementById(`graph-${logicalId}`)) {
                node.style.cursor = 'pointer';
                node.onclick = (e) => {
                    e.stopPropagation();
                    this.renderGraph(logicalId);
                    document.getElementById('graph-title').textContent = logicalId;
                    document.getElementById('back-btn').classList.remove('hidden');

                    // Enable clickable header
                    const headerContainer = document.getElementById('graph-header-container');
                    headerContainer.classList.add('cursor-pointer', 'hover:bg-slate-100');
                };
            } else {
                // Leaf node in main graph ? Check mapping
                const mapping = State.nodeMapping[logicalId];
                if (mapping) {
                    node.style.cursor = 'pointer';
                    node.onclick = (e) => {
                        e.stopPropagation();
                        this.loadFileFromMapping(mapping);
                    }
                }
            }
        });
    },

    bindDetailGraphInteractivity(graphId) {
        const navMapping = State.codeNavigation[graphId];
        if (!navMapping) return;

        const nodes = document.querySelectorAll('#graph-container g.node');
        nodes.forEach(node => {
            const nodeId = node.id;
            const logicalId = this.extractLogicalId(nodeId);

            // Try to match logical ID to navigation keyword
            let keyword = navMapping[logicalId];

            if (keyword) {
                node.style.cursor = 'pointer';
                node.onclick = async (e) => {
                    e.stopPropagation();

                    // Ensure the file is loaded and active first
                    const mapping = State.nodeMapping[graphId];
                    if (mapping) {
                        await this.loadFileFromMapping(mapping);
                    }

                    // Then navigate and highlight
                    this.navigateToCodeAndHighlight(keyword);
                };

                // Hover effect
                const shape = node.querySelector('rect, path, polygon');
                let originalStroke = shape ? shape.getAttribute('stroke') : '#cbd5e1';
                let originalWidth = shape ? shape.getAttribute('stroke-width') : '1';

                node.onmouseenter = () => {
                    if (shape) {
                        shape.setAttribute('stroke', '#3b82f6');
                        shape.setAttribute('stroke-width', '3');
                    }
                };
                node.onmouseleave = () => {
                    if (shape) {
                        shape.setAttribute('stroke', originalStroke);
                        shape.setAttribute('stroke-width', originalWidth);
                    }
                };
            }
        });

        // Also ensure the corresponding file is loaded
        const mapping = State.nodeMapping[graphId];
        if (mapping) {
            this.loadFileFromMapping(mapping);
        }
    },

    extractLogicalId(nodeId) {
        // Mermaid ID format: flowchart-{ID}-{NUMBER}
        const match = nodeId.match(/flowchart-([^-]+)-\d+/);
        return match ? match[1] : nodeId;
    },

    async loadFileFromMapping(mapping) {
        // Direct handling: Load content and activate/open tab.
        // We do NOT rely on simulating a click on the file tree anymore for logic execution.
        const filename = mapping.filename;

        try {
            // Check if we need to fetch content (simulating what handleFileSelect does but ensuring we have it)
            // Actually, handleFileSelect fetches if content is missing.

            await this.handleFileSelect({
                path: mapping.path,
                name: filename,
                lang: mapping.lang,
                content: null // Let handleFileSelect fetch it if needed
            });

        } catch (e) {
            console.error(e);
        }
    },

    navigateToCodeAndHighlight(keyword) {
        if (!Editor.instance) return;

        const model = Editor.instance.getModel();
        if (!model) return;

        const matches = model.findMatches(keyword, false, false, false, null, true);
        if (matches && matches.length > 0) {
            const range = matches[0].range;

            // Reveal line
            Editor.instance.revealLineInCenter(range.startLineNumber);

            // Select it (optional)
            Editor.instance.setSelection(range);

            // Highlight decoration
            this.highlightRange(range);
        } else {
            console.warn(`Keyword not found: ${keyword}`);
        }
    },

    highlightRange(range) {
        // Use deltaDecorations to add a class
        const decorations = [{
            range: range,
            options: {
                isWholeLine: true,
                className: 'myLineDecoration',
                inlineClassName: 'myInlineDecoration'
            }
        }];

        const oldDecorations = this.currentDecorations || [];
        this.currentDecorations = Editor.instance.deltaDecorations(oldDecorations, decorations);

        // Remove after animation (2s)
        setTimeout(() => {
            this.currentDecorations = Editor.instance.deltaDecorations(this.currentDecorations, []);
        }, 2000);
    },

    // --- File & Editor Logic (Merged) ---

    openFiles: [], // Array of file objects { path, name, lang, content }
    pendingFileLoads: new Map(), // Track in-progress loads to prevent duplicates

    async handleFileSelect(file, skipSave = false) {
        console.log('Selected file:', file);

        // 1. Check if file is already open
        let existingFileIndex = this.openFiles.findIndex(f => {
            if (f.path === file.path) return true;
            if (f.name === file.name) return true;
            return false;
        });

        if (existingFileIndex !== -1) {
            this.activateTab(this.openFiles[existingFileIndex], skipSave);
            return;
        }

        // 2. Check if file is currently loading
        if (this.pendingFileLoads.has(file.path)) {
            console.log('File already loading, waiting:', file.path);
            await this.pendingFileLoads.get(file.path);

            // Re-check existence after wait
            existingFileIndex = this.openFiles.findIndex(f => f.path === file.path);
            if (existingFileIndex !== -1) {
                this.activateTab(this.openFiles[existingFileIndex], skipSave);
            }
            return;
        }

        // 3. Load new file
        const loadPromise = (async () => {
            try {
                // If content not provided in file object, fetch it
                if (!file.content) {
                    file.content = await this.fetchFile(file.path);
                }
                this.openFiles.push(file);
                this.renderTabs();
                this.activateTab(file, skipSave);
            } catch (error) {
                console.error('Error loading file:', error);
                Editor.setModel(`Error loading file: ${error.message}`, 'plaintext', null);
            }
        })();

        this.pendingFileLoads.set(file.path, loadPromise);

        try {
            await loadPromise;
        } finally {
            this.pendingFileLoads.delete(file.path);
        }
    },

    activateTab(file, skipSave = false) {
        State.currentFile = file.path;
        Editor.setModel(file.content, this.mapLanguage(file.lang), file.path);
        this.updateBreadcrumbs(file.path);
        this.renderTabs();

        if (!skipSave) this.saveState();
    },

    closeTab(path, event) {
        if (event) event.stopPropagation();

        const index = this.openFiles.findIndex(f => f.path === path);
        if (index === -1) return;

        const wasActive = this.openFiles[index].path === State.currentFile;
        this.openFiles.splice(index, 1);

        this.renderTabs();

        // Save state immediately after closing
        this.saveState();

        if (this.openFiles.length === 0) {
            Editor.setModel('// Select a file to view', 'plaintext', null);
            State.currentFile = null;
            this.updateBreadcrumbs('Select a file to view');
            this.saveState(); // Save empty state
        } else if (wasActive) {
            // Switch to the previous one, or the next one
            const nextFile = this.openFiles[index] || this.openFiles[index - 1];
            this.activateTab(nextFile);
        }
    },

    mapLanguage(lang) {
        const map = {
            'js': 'javascript',
            'py': 'python',
            'html': 'html',
            'css': 'css',
            'sql': 'sql',
            'json': 'json'
        };
        return map[lang] || lang || 'plaintext';
    },

    async fetchFile(path) {
        const response = await fetch(path);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.text();
    },

    updateBreadcrumbs(path) {
        const el = document.getElementById('breadcrumb-path');
        if (el) el.textContent = path;
    },

    renderTabs() {
        const container = document.getElementById('editor-tabs');
        if (!container) return;

        container.innerHTML = ''; // Clear

        this.openFiles.forEach((file, index) => {
            const div = document.createElement('div');
            const isActive = file.path === State.currentFile ? 'active' : '';
            div.className = `tab-item ${isActive}`;

            // Drag and Drop Attributes
            div.setAttribute('draggable', 'true');
            div.dataset.index = index;

            div.innerHTML = `
                <span class="file-icon ${this.getFileIconClass(file.name)}"></span>
                <span class="mr-2">${file.name}</span>
                <span class="tab-close">×</span>
            `;

            // Click to activate (Left click)
            div.addEventListener('click', () => {
                this.activateTab(file);
            });

            // Middle click to close (auxclick or mousedown checking button)
            div.addEventListener('mouseup', (e) => {
                if (e.button === 1) { // Middle button
                    e.preventDefault();
                    this.closeTab(file.path, e);
                }
            });

            // Close button
            const closeBtn = div.querySelector('.tab-close');
            closeBtn.addEventListener('click', (e) => {
                this.closeTab(file.path, e);
            });

            // Drag Events
            div.addEventListener('dragstart', (e) => this.handleTabDragStart(e, index));
            div.addEventListener('dragover', (e) => e.preventDefault()); // Allow drop
            div.addEventListener('dragenter', (e) => e.preventDefault());
            div.addEventListener('drop', (e) => this.handleTabDrop(e, index));

            container.appendChild(div);
        });
    },

    handleTabDragStart(e, index) {
        e.dataTransfer.setData('text/plain', index);
        e.dataTransfer.effectAllowed = 'move';
        // Add dragging class for visual feedback (optional if we want CSS styling)
        e.target.classList.add('dragging');

        // Remove dragging class on end
        e.target.addEventListener('dragend', () => {
            e.target.classList.remove('dragging');
        }, { once: true });
    },

    handleTabDrop(e, targetIndex) {
        e.preventDefault();
        const fromIndex = parseInt(e.dataTransfer.getData('text/plain'));

        if (fromIndex !== targetIndex) {
            // Reorder array
            const movedItem = this.openFiles.splice(fromIndex, 1)[0];
            this.openFiles.splice(targetIndex, 0, movedItem);

            this.renderTabs();
            this.saveState();
        }
    },

    saveState() {
        if (this.isRestoring) return; // Prevent overwriting state during restoration

        const stateToSave = {
            openFiles: this.openFiles.map(f => ({
                path: f.path,
                name: f.name,
                lang: f.lang
                // NOT saving content to keep localStorage light
            })),
            currentFile: State.currentFile
        };
        localStorage.setItem('appState', JSON.stringify(stateToSave));
    },

    loadState(cachedState = null) {
        return new Promise((resolve) => {
            const saved = cachedState || localStorage.getItem('appState');
            if (saved) {
                try {
                    const parsed = JSON.parse(saved);
                    if (parsed.openFiles && Array.isArray(parsed.openFiles)) {
                        this.isRestoring = true;

                        // Re-open files. 
                        const promise = parsed.openFiles.reduce(async (prevPromise, file) => {
                            await prevPromise;
                            return this.handleFileSelect(file, true);
                        }, Promise.resolve());

                        promise.then(() => {
                            // After all opened, activate the correct one
                            if (parsed.currentFile) {
                                const fileToActivate = this.openFiles.find(f => f.path === parsed.currentFile);
                                if (fileToActivate) {
                                    this.activateTab(fileToActivate, true);
                                }
                            }
                        }).finally(() => {
                            this.isRestoring = false;
                            resolve(); // Resolve when done
                        });
                        return; // Async flow started
                    }
                } catch (e) {
                    console.error('Failed to load app state:', e);
                }
            }
            resolve(); // Nothing to restore or error, resolve immediately
        });
    },

    getFileIconClass(filename) {
        if (filename.endsWith('.py')) return 'icon-py';
        if (filename.endsWith('.sql')) return 'icon-sql';
        if (filename.endsWith('.html')) return 'icon-html';
        return 'icon-file';
    }
};

document.addEventListener('DOMContentLoaded', () => {
    App.init();
});
