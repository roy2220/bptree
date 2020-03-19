(() => {
"use strict";

function loadBPTreeModule(callback) {
    const go = new Go();

    WebAssembly.instantiateStreamingCompressed = async (resp, importObject) => {
        const sourceCompressed = await (await resp).arrayBuffer();
        const source = pako.inflate(sourceCompressed);
        return await WebAssembly.instantiate(source, importObject);
    };

    WebAssembly.instantiateStreamingCompressed(fetch("./bptree.wasm.gz"), go.importObject).then((result) => {
        go.run(result.instance);
        callback();
    });
}

function updateSvg() {
        const svg = document.getElementById("svg");
        const arrayTree = eval(BPTree.dump());
        const dotScript = generateDotScript(arrayTree);
        // console.log(dotScript);
        const html = Viz(dotScript, "svg");
        svg.innerHTML = html.replace(/(\<svg width=")[^"]+/i, "$1100%");
}

function generateDotScript(arrayTree) {
    let nodePathsOfLevels = [];
    let prevLeafPath = ""
    var f;

    f = (node, nodePath, level) => {
        if (level == nodePathsOfLevels.length) {
            nodePathsOfLevels.push([]);
        }

        nodePathsOfLevels[level].push(nodePath);

        let lines = [] ;
        const i = nodePath.lastIndexOf("_");

        if (i >= 0) {
            lines.push("  " + nodePath.substring(0, i) + ":c" + nodePath.substring(i+1) + " -> " + nodePath);
        }

        let nodeIsLeaf = true;

        for (let x of node) {
            if (x instanceof Array) {
                nodeIsLeaf = false;
                break;
            }
        }

        if (nodeIsLeaf) {
            if (prevLeafPath != "") {
                lines.push("  " + prevLeafPath + " -> " + nodePath);
                lines.push("  " + nodePath + " -> " + prevLeafPath);
            }

            prevLeafPath = nodePath;
        }

        let line = "  " + nodePath + " [label = \"";
        let childIndex = 0;

        for (let x of node) {
            if (x instanceof Array) {
                line += "<c" + childIndex.toString() + ">|"
                childIndex++
            } else {
                line += x.toString() + "|"
            }
        }

        line = line.substr(0, line.length - 1);
        line += "\"]"
        lines.push(line);

        if (!nodeIsLeaf) {
            childIndex = 0;

            for (let x of node) {
                if (x instanceof Array) {
                    Array.prototype.push.apply(lines, f(x, nodePath + "_" + childIndex.toString(), level + 1));
                    childIndex++;
                }
            }
        }

        return lines;
    };

    let lines = [
        "digraph G {",
        "  node [shape = record]"
    ];

    if (arrayTree.length >= 1) {
        Array.prototype.push.apply(lines, f(arrayTree, "n", 0));

        for (let nodePathsOfLevel of nodePathsOfLevels) {
            lines.push("  { rank = same; "+ nodePathsOfLevel.join("; ") +"; }");
        }
    }

    lines.push("}")
    return lines.join("\n")
}

let curMaxDegree = 0;
const opStack = [];
let opStackHeight = 0;

function loadOpHistory() {
    let s = document.location.hash;

    if (s.startsWith("#")) {
        s = s.substr(1);
    }

    const a = s.split(",");

    if (a.length == 1 && a[0] == "") {
        resetOpHistory(4);
    } else{
        curMaxDegree = parseFloat(a[0]);
        BPTree.init(curMaxDegree);
        document.querySelector("input[name=\"maxDegree\"][value=\""+curMaxDegree.toString()+"\"]").checked = true;

        for (let t of a.slice(1)) {
            const op = parseFloat(t);

            if (op < 0) {
                const key = -op;
                BPTree.deleteKey(key)
            } else {
                const key = op;
                BPTree.addKey(key)
            }

            opStack[opStackHeight] = op;
            opStackHeight++;
        }

        updateSvg();
    }
}

function resetOpHistory(maxDegree) {
    BPTree.init(maxDegree);
    curMaxDegree = maxDegree;
    updateSvg();
    opStack.length = 0;
    opStackHeight = 0;
    document.location.hash = "#" + maxDegree.toString();
    document.querySelector("input[name=\"maxDegree\"][value=\""+maxDegree.toString()+"\"]").checked = true;
    document.querySelector("input[name=\"key\"]").value = 1;
}

function doOp(op) {
    if (op < 0) {
        const key = -op;

        if (!BPTree.deleteKey(key)) {
            return false
        }
    } else {
        const key = op;

        if (!BPTree.addKey(key)) {
            return false
        }
    }

    updateSvg();

    if (opStack.length > opStackHeight) {
        opStack.length = opStackHeight;
    }

    opStack[opStackHeight] = op;
    opStackHeight++;
    const s = "," + (op>0?"+":"") + op.toString();
    document.location.hash += s;
    return true
}

function undoOp() {
    if (opStackHeight == 0) {
        return;
    }

    const op = opStack[opStackHeight-1];
    opStackHeight--;
    const s = "," + (op>0?"+":"") + op.toString();
    document.location.hash = document.location.hash.substring(0, document.location.hash.length - s.length);
    BPTree.init(curMaxDegree);

    for (let op of opStack.slice(0, opStackHeight)) {
        if (op < 0) {
            const key = -op;
            BPTree.deleteKey(key);
        } else {
            const key = op;
            BPTree.addKey(key);
        }
    }

    updateSvg();
}

function redoOp() {
    if (opStack.length == opStackHeight) {
        return;
    }

    opStackHeight++;
    const op = opStack[opStackHeight-1];

    if (op < 0) {
        const key = -op;
        BPTree.deleteKey(key);
    } else {
        const key = op;
        BPTree.addKey(key);
    }

    updateSvg();
    const s = "," + (op>0?"+":"") + op.toString();
    document.location.hash += s;
}

document.addEventListener("DOMContentLoaded", () => {
    loadBPTreeModule(() => {
        for (let e of document.querySelectorAll("input[name=\"maxDegree\"]")) {
            const e2 = e;

            e2.addEventListener("click", () => {
                const maxDegree = parseInt(e2.value);
                resetOpHistory(maxDegree);
            });
        }

        document.getElementById("btnAddKey").addEventListener("click", () => {
            const e = document.querySelector("input[name=\"key\"]")
            const key = parseFloat(e.value);

            if (key <= 0) {
                return;
            }

            if (doOp(key)) {
                if (key.toFixed(0) == key.toString()) {
                    e.value = key+1;
                }
            }
        });

        document.getElementById("btnDeleteKey").addEventListener("click", () => {
            const e = document.querySelector("input[name=\"key\"]")
            const key = parseFloat(e.value);

            if (key <= 0) {
                return
            }

            doOp(-key);
        });

        document.getElementById("btnUndo").addEventListener("click", () => {
            undoOp();
        });

        document.getElementById("btnRedo").addEventListener("click", () => {
            redoOp();
        });

        loadOpHistory();
    });
}, false);

})();
