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

function updateSvg(newKey) {
        const svg = document.getElementById("svg");
        const arrayTree = eval(BPTree.dump());
        const dotScript = generateDotScript(arrayTree, newKey);
        // console.log(dotScript);
        const html = Viz(dotScript, "svg");
        svg.innerHTML = html.replace(/(\<svg width=")[^"]+/i, "$1100%");
}

function generateDotScript(arrayTree, newKey) {
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
                console.log(x, newKey);
                if (nodeIsLeaf && x == newKey) {
                    lines.push("  new_key [label = \"new key\", shape = plaintext]");
                    lines.push("  new_key -> " + nodePath + ":nk");
                    line += "<nk>" + x.toString() + "|";
                } else {
                    line += x.toString() + "|";
                }
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
        let newKey = null;

        for (let t of a.slice(1)) {
            const op = parseFloat(t);

            if (op < 0) {
                const key = -op;
                BPTree.deleteKey(key);
                newKey = null;
            } else {
                const key = op;
                BPTree.addKey(key);
                newKey = key;
            }

            opStack[opStackHeight] = op;
            opStackHeight++;
        }

        updateSvg(newKey);
        const maxKey = BPTree.findMax();

        if (maxKey != null) {
            document.querySelector("input[name=\"key\"]").value = (maxKey+1).toFixed(0);
        }
    }
}

function resetOpHistory(maxDegree) {
    BPTree.init(maxDegree);
    curMaxDegree = maxDegree;
    updateSvg(null);
    opStack.length = 0;
    opStackHeight = 0;
    document.location.hash = "#" + maxDegree.toString();
    document.querySelector("input[name=\"maxDegree\"][value=\""+maxDegree.toString()+"\"]").checked = true;

    if (document.querySelector("input[name=\"random-new-key\"]").checked) {
        setRandomNewKey();
    } else {
        document.querySelector("input[name=\"key\"]").value = 1;
    }
}

function doOp(op) {
    var newKey;

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

        newKey = op;
    }

    updateSvg(newKey);

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
    let newKey = null;

    for (let op of opStack.slice(0, opStackHeight)) {
        if (op < 0) {
            const key = -op;
            BPTree.deleteKey(key);
            newKey = null;
        } else {
            const key = op;
            BPTree.addKey(key);
            newKey = key;
        }
    }

    updateSvg(newKey);
}

function redoOp() {
    if (opStack.length == opStackHeight) {
        return;
    }

    opStackHeight++;
    const op = opStack[opStackHeight-1];
    var newKey;

    if (op < 0) {
        const key = -op;
        BPTree.deleteKey(key);
        newKey = null;
    } else {
        const key = op;
        BPTree.addKey(key);
        newKey = key;
    }

    updateSvg(newKey);
    const s = "," + (op>0?"+":"") + op.toString();
    document.location.hash += s;
}

function setRandomNewKey() {
    let s = [];
    let n = 1;

    for (const nn of [10, 100, 1000]) {
        const maxKey = BPTree.findMax();

        if (maxKey != null && maxKey > nn) {
            continue;
        }

        for (; n <= nn; n++) {
            if (!BPTree.hasKey(n)) {
                s.push(n);
            }
        }

        if (s.length >= 1) {
            document.querySelector("input[name=\"key\"]").value = s[Math.floor(s.length*Math.random())];
            return
        }
    }
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

        document.querySelector("input[name=\"random-new-key\"]").addEventListener("click", () => {
            if (document.querySelector("input[name=\"random-new-key\"]").checked) {
                setRandomNewKey();
            }
        });

        const tips = document.getElementById("tips");

        document.getElementById("add-key").addEventListener("click", () => {
            const e = document.querySelector("input[name=\"key\"]")
            const key = parseFloat(e.value);

            if (key <= 0) {
                tips.textContent = "keys must be positive numbers";
                return;
            }

            if (!doOp(key)) {
                tips.textContent = "key `"+key.toString()+"` already exists";
                return;
            }

            tips.textContent = "";

            if (document.querySelector("input[name=\"random-new-key\"]").checked) {
                setRandomNewKey();
            } else {
                const maxKey = BPTree.findMax();
                document.querySelector("input[name=\"key\"]").value = (maxKey+1).toFixed(0);
            }
        });

        document.getElementById("delete-key").addEventListener("click", () => {
            const e = document.querySelector("input[name=\"key\"]")
            const key = parseFloat(e.value);

            if (key <= 0) {
                tips.textContent = "keys must be positive numbers";
                return
            }

            if (!doOp(-key)) {
                tips.textContent = "key `"+key.toString()+"` doesn't exist";
                return
            }

            tips.textContent = "";
        });

        document.getElementById("undo").addEventListener("click", () => {
            undoOp();
        });

        document.getElementById("redo").addEventListener("click", () => {
            redoOp();
        });

        document.getElementById("reset").addEventListener("click", () => {
            resetOpHistory(curMaxDegree);
        });

        loadOpHistory();
        document.getElementById("content-loading").hidden = true;
        document.getElementById("content").hidden = false;
    });
}, false);

})();
