import os
import base64
import tempfile
import uuid
from flask import Flask, request, render_template, jsonify
from reconcile import reconcile

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB limit

ALLOWED_EXT = {".xls", ".xlsx"}


def _allowed(filename):
    return os.path.splitext(filename.lower())[1] in ALLOWED_EXT


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/reconcile", methods=["POST"])
def run_reconcile():
    if "gstr2b_file" not in request.files or "tally_file" not in request.files:
        return jsonify({"error": "Both files are required. Please upload both files."}), 400

    gstr2b_file = request.files["gstr2b_file"]
    tally_file  = request.files["tally_file"]

    if not gstr2b_file.filename or not tally_file.filename:
        return jsonify({"error": "Please select both files before clicking Run."}), 400

    if not _allowed(gstr2b_file.filename):
        return jsonify({"error": "Portal file must be an Excel file (.xls or .xlsx)."}), 400
    if not _allowed(tally_file.filename):
        return jsonify({"error": "Tally file must be an Excel file (.xls or .xlsx)."}), 400

    tmp      = tempfile.gettempdir()
    job_id   = uuid.uuid4().hex[:10]
    g_ext    = os.path.splitext(gstr2b_file.filename)[1].lower()
    t_ext    = os.path.splitext(tally_file.filename)[1].lower()
    g_path   = os.path.join(tmp, f"{job_id}_gstr2b{g_ext}")
    t_path   = os.path.join(tmp, f"{job_id}_tally{t_ext}")
    out_path = os.path.join(tmp, f"{job_id}_out.xlsx")

    try:
        gstr2b_file.save(g_path)
        tally_file.save(t_path)

        _, summary, tables = reconcile(g_path, t_path, output_path=out_path)

        with open(out_path, "rb") as f:
            file_b64 = base64.b64encode(f.read()).decode("utf-8")

        return jsonify({
            "success": True,
            "summary": summary,
            "tables":  tables,
            "file":    file_b64,
        })

    except Exception as e:
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500

    finally:
        for p in [g_path, t_path, out_path]:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
