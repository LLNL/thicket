#!/bin/sh

case *"$PWD"* in
    "$PYTHONPATH")
        ;;

    *)
        PYTHONPATH=$PWD:$PYTHONPATH
        ;;
esac

python setup.py build_ext --inplace