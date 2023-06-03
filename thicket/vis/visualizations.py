from os import path
from os.path import dirname

from IPython.core.magic import Magics, magics_class, line_magic
from hatchet.external import Roundtrip as RT


def _thicket_to_json(data):
    return data.to_json()


def _df_to_json(data):
    return data.to_json(orient="records")


def _basic_to_json(data):
    import json

    return json.dumps(data)


vis_dir = dirname(path.abspath(__file__))


@magics_class
class EnsembleVis(Magics):
    def __init__(self, shell):
        super(EnsembleVis, self).__init__(shell)
        self.vis_dist = path.join(vis_dir, "static")

    @line_magic
    def metadata_vis(self, line):
        args = line.split(" ")
        RT.load_webpack(path.join(self.vis_dist, "pcp_bundle.html"), cache=False)
        RT.var_to_js(
            args[0], "thicket_ensemble", watch=False, to_js_converter=_thicket_to_json
        )

        if len(args) > 1:
            RT.var_to_js(
                args[1], "metadata_dims", watch=False, to_js_converter=_basic_to_json
            )

        if len(args) > 2:
            RT.var_to_js(
                args[2], "focus_node", watch=False, to_js_converter=_basic_to_json
            )

        RT.initialize()

    @line_magic
    def topdown_analysis(self, line):
        args = line.split(" ")
        RT.load_webpack(path.join(self.vis_dist, "topdown_bundle.html"), cache=False)
        RT.var_to_js(
            args[0], "topdown_data", watch=False, to_js_converter=_thicket_to_json
        )

        RT.initialize()


def load_ipython_extension(ipython):
    ipython.register_magics(EnsembleVis)
