/**
 * File Tree Component
 */

const FileTree = {
    // 模拟的文件列表，后续可以改为动态获取
    files: [
        { name: '全域风险团伙审核_召回_汇总.py', type: 'file', lang: 'python', path: '../全域风险团伙审核_召回_汇总.py' },
        { name: '全域风险团伙审核_召回_团伙规则过滤_虚假宣传.py', type: 'file', lang: 'python', path: '../全域风险团伙审核_召回_团伙规则过滤_虚假宣传.py' },
        { name: '全域风险团伙审核_召回_团伙规则过滤_劣质.py', type: 'file', lang: 'python', path: '../全域风险团伙审核_召回_团伙规则过滤_劣质.py' },
        { name: '全域风险团伙审核_召回_团伙规则过滤_假货.py', type: 'file', lang: 'python', path: '../全域风险团伙审核_召回_团伙规则过滤_假货.py' },
        { name: '全域风险团伙审核_召回_团伙种子过滤_虚假宣传.sql', type: 'file', lang: 'sql', path: '../全域风险团伙审核_召回_团伙种子过滤_虚假宣传.sql' },
        { name: '全域风险团伙审核_召回_团伙基础数据.sql', type: 'file', lang: 'sql', path: '../全域风险团伙审核_召回_团伙基础数据.sql' },
        { name: '底线违规商品映射解析_假货.py', type: 'file', lang: 'python', path: '../底线违规商品映射解析_假货.py' }
    ],

    container: null,
    onFileSelect: null,

    init(containerId, onFileSelectCallback) {
        this.container = document.getElementById(containerId);
        this.onFileSelect = onFileSelectCallback;
        this.render();
    },

    render() {
        if (!this.container) return;
        this.container.innerHTML = this.buildTreeHTML(this.files, 0);
        this.attachEvents();
    },

    buildTreeHTML(nodes, level) {
        let html = '';
        nodes.forEach(node => {
            const paddingLeft = level * 16 + 10; // Reduced base padding, allowing for arrow

            if (node.type === 'folder') {
                html += `
                    <div class="file-item folder expanded" style="padding-left: ${paddingLeft}px" data-path="${node.path || ''}">
                        <span class="tree-arrow expanded"></span>
                        <span class="file-icon icon-folder"></span>
                        <span class="file-name">${node.name}</span>
                    </div>
                `;
                if (node.children) {
                    html += `<div class="folder-children expanded">${this.buildTreeHTML(node.children, level + 1)}</div>`;
                }
            } else {
                let iconClass = 'icon-file';
                if (node.name.endsWith('.py')) iconClass = 'icon-py';
                if (node.name.endsWith('.sql')) iconClass = 'icon-sql';
                if (node.name.endsWith('.html')) iconClass = 'icon-html';

                // Indent file to align with folder text (arrow width approx 16px)
                const filePadding = paddingLeft + 16;

                html += `
                    <div class="file-item file" style="padding-left: ${filePadding}px" data-path="${node.path}" data-lang="${node.lang || 'plaintext'}">
                        <span class="file-icon ${iconClass}"></span>
                        <span class="file-name">${node.name}</span>
                    </div>
                `;
            }
        });
        return html;
    },

    attachEvents() {
        // File Click
        this.container.querySelectorAll('.file-item.file').forEach(item => {
            item.addEventListener('click', (e) => {
                // Remove active class from all
                this.container.querySelectorAll('.file-item').forEach(el => el.classList.remove('active'));
                // Add to clicked
                e.currentTarget.classList.add('active');

                const path = e.currentTarget.dataset.path;
                const lang = e.currentTarget.dataset.lang;
                const name = e.currentTarget.querySelector('.file-name').textContent;

                if (this.onFileSelect) {
                    this.onFileSelect({ path, lang, name });
                }
            });
        });

        // Folder Click
        this.container.querySelectorAll('.file-item.folder').forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();

                // Toggle arrow state
                const arrow = item.querySelector('.tree-arrow');
                if (arrow) arrow.classList.toggle('expanded');

                // Toggle children visibility
                const childrenContainer = item.nextElementSibling;
                if (childrenContainer && childrenContainer.classList.contains('folder-children')) {
                    childrenContainer.classList.toggle('collapsed');
                }
            });
        });
    },

    selectFileByname(filename) {
        // Find matching item
        const items = this.container.querySelectorAll('.file-item.file');
        for (let item of items) {
            const nameSpan = item.querySelector('.file-name');
            if (nameSpan && nameSpan.textContent === filename) {
                // Trigger click
                item.click();
                item.scrollIntoView({ block: 'center' });
                return true;
            }
        }
        return false;
    },

    highlightFileByPath(path) {
        // Remove active class from all
        this.container.querySelectorAll('.file-item').forEach(el => el.classList.remove('active'));

        // Find and highlight
        const item = this.container.querySelector(`.file-item[data-path="${path}"]`);
        if (item) {
            item.classList.add('active');
            item.scrollIntoView({ block: 'center' });
            return true;
        }
        return false;
    }
};

