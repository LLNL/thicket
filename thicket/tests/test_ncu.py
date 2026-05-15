# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from hatchet.node import Node

from thicket.ncu import (
    _match_call_trace_regex,
    _match_kernel_str_to_cali,
    _multi_match_fallback_similarity,
)


def test_match_call_trace_regex():

    # Base_CUDA variant
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["RAJAPerf", "Basic", "Basic_DAXPY"],
        "void rajaperf::basic::daxpy<(unsigned long)128>(double *, double *, double, long)",
        debug=False,
    )
    assert kernel_str == "daxpy"

    # lambda_CUDA variant
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["RAJAPerf", "Polybench", "Polybench_ATAX"],
        "void rajaperf::polybench::poly_atax_lam<(unsigned long)128, void rajaperf::polybench::POLYBENCH_ATAX::runCudaVariantImpl<(unsigned long)128>(rajaperf::VariantID)::[lambda(long) (instance 2)]>(long, T2)",
        debug=False,
    )
    assert kernel_str == "poly_atax_lam"

    # RAJA_CUDA variant
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["RAJAPerf", "Apps", "Apps_ENERGY"],
        "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)128, (int)0>, RAJA::cuda::MaxOccupancyConcretizer, (unsigned long)1, (bool)1>, (unsigned long)1, RAJA::Iterators::numeric_iterator<long, long, long *>, void rajaperf::apps::ENERGY::runCudaVariantImpl<(unsigned long)128>(rajaperf::VariantID)::[lambda() (instance 1)]::operator ()() const::[lambda(long) (instance 4)], long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)128, (int)0>, (unsigned long)128>(T4, T3, T5)",
        debug=False,
    )
    assert kernel_str == "ENERGY"


def test_match_kernel_str_to_cali():
    # RAJA_CUDA variant
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["RAJAPerf", "Apps", "Apps_ENERGY"],
        "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)128, (int)0>, RAJA::cuda::MaxOccupancyConcretizer, (unsigned long)1, (bool)1>, (unsigned long)1, RAJA::Iterators::numeric_iterator<long, long, long *>, void rajaperf::apps::ENERGY::runCudaVariantImpl<(unsigned long)128>(rajaperf::VariantID)::[lambda() (instance 1)]::operator ()() const::[lambda(long) (instance 4)], long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)128, (int)0>, (unsigned long)128>(T4, T3, T5)",
        debug=False,
    )
    # Test multi-instance (for energy4)
    node_set = [
        Node({"name": "RAJAPerf", "type": "function"}),
        Node({"name": "Apps", "type": "function"}),
        Node({"name": "Apps_ENERGY", "type": "function"}),
        Node({"name": "cudaLaunchKernel", "type": "function"}),
        # energy1
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#1}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#1}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
        # energy2
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#2}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#2}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
        # energy3
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#3}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#3}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
        # energy4
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#4}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#4}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
        # energy5
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#5}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#5}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
        # energy6
        Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#6}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#6}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ),
    ]
    matched_nodes = _match_kernel_str_to_cali(
        node_set, kernel_str, instance_num, instance_exists
    )
    assert len(matched_nodes) == 1
    # energy4
    assert (
        matched_nodes[0].frame["name"]
        == Node(
            {
                "name": "void RAJA::policy::cuda::impl::forall_cuda_kernel<RAJA::policy::cuda::cuda_exec_explicit<RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, RAJA::cuda::MaxOccupancyConcretizer, 1ul, true>, 1ul, RAJA::Iterators::numeric_iterator<long, long, long*>, void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#4}, long, RAJA::iteration_mapping::Direct, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, 128, 0>, 128ul>(void rajaperf::apps::ENERGY::runCudaVariantImpl<128ul>(rajaperf::VariantID)::{lambda()#1}::operator()() const::{lambda(long)#4}, RAJA::Iterators::numeric_iterator<long, long, long*>, long)",
                "type": "kernel",
            }
        ).frame["name"]
    )


def test_multi_match_fallback_similarity():
    # CUB kernels
    demangled_kernel_name = "void cub::DeviceRadixSortUpsweepKernel<cub::DeviceRadixSortPolicy<double, cub::NullType, int>::Policy700, (bool)1, (bool)0, double, int>(const T4 *, T5 *, T5, int, int, cub::GridEvenShare<T5>)"
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["RAJAPerf", "Algorithm", "Algorithm_SORT", "DeviceRadixSortUpsweepKernel"],
        demangled_kernel_name=demangled_kernel_name,
        debug=False,
    )
    node_set = [
        Node({"name": "RAJAPerf", "type": "function"}),
        Node({"name": "Algorithm", "type": "function"}),
        Node({"name": "Algorithm_SORT", "type": "function"}),
        Node({"name": "cudaLaunchKernel", "type": "function"}),
        # "false, false" wrong match
        Node(
            {
                "name": "void cub::DeviceRadixSortUpsweepKernel<cub::DeviceRadixSortPolicy<double, cub::NullType, int>::Policy700, false, false, double, int>(double const*, int*, int, int, int, cub::GridEvenShare<int>)",
                "type": "kernel",
            }
        ),
        # "true, false" correct match
        Node(
            {
                "name": "void cub::DeviceRadixSortUpsweepKernel<cub::DeviceRadixSortPolicy<double, cub::NullType, int>::Policy700, true, false, double, int>(double const*, int*, int, int, int, cub::GridEvenShare<int>)",
                "type": "kernel",
            }
        ),
    ]
    matched_nodes = _match_kernel_str_to_cali(
        node_set, kernel_str, instance_num, instance_exists
    )
    matched_node = _multi_match_fallback_similarity(
        matched_nodes, demangled_kernel_name, debug=False
    )
    assert (
        matched_node.frame["name"]
        == "void cub::DeviceRadixSortUpsweepKernel<cub::DeviceRadixSortPolicy<double, cub::NullType, int>::Policy700, true, false, double, int>(double const*, int*, int, int, int, cub::GridEvenShare<int>)"
    )


def test_match_call_trace_regex_kripke_uses_unsafe_fallback():
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["main", "Solve", "solve", "LTimes", "ltimes_kernel_0", "ltimessdom_kernel"],
        "void RAJA::internal::CudaKernelLauncherFixed<(int)1024, (int)1, RAJA::internal::LoopData<camp::tuple<RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Moment, long, Kripke::Moment *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Direction, long, Kripke::Direction *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Group, long, Kripke::Group *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Zone, long, Kripke::Zone *>, long>>, camp::tuple<>, camp::resources::v1::Cuda, void LTimesSdom::operator ()<Kripke::ArchLayoutT<Kripke::ArchT_CUDA, Kripke::LayoutT_ZGD>>(T1, Kripke::SdomId, const Kripke::Core::Set &, const Kripke::Core::Set &, const Kripke::Core::Set &, const Kripke::Core::Set &, Kripke::Core::Field<double, Kripke::Direction, Kripke::Group, Kripke::Zone> &, Kripke::Core::Field<double, Kripke::Moment, Kripke::Group, Kripke::Zone> &, Kripke::Core::Field<double, Kripke::Moment, Kripke::Direction> &, unsigned long) const::[lambda(Kripke::Moment, Kripke::Direction, Kripke::Group, Kripke::Zone) (instance 1)]>, RAJA::internal::CudaStatementListExecutor<RAJA::internal::LoopData<camp::tuple<RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Moment, long, Kripke::Moment *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Direction, long, Kripke::Direction *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Group, long, Kripke::Group *>, long>, RAJA::Span<RAJA::Iterators::numeric_iterator<Kripke::Zone, long, Kripke::Zone *>, long>>, camp::tuple<>, camp::resources::v1::Cuda, void LTimesSdom::operator ()<Kripke::ArchLayoutT<Kripke::ArchT_CUDA, Kripke::LayoutT_ZGD>>(T1, Kripke::SdomId, const Kripke::Core::Set &, const Kripke::Core::Set &, const Kripke::Core::Set &, const Kripke::Core::Set &, Kripke::Core::Field<double, Kripke::Direction, Kripke::Group, Kripke::Zone> &, Kripke::Core::Field<double, Kripke::Moment, Kripke::Group, Kripke::Zone> &, Kripke::Core::Field<double, Kripke::Moment, Kripke::Direction> &, unsigned long) const::[lambda(Kripke::Moment, Kripke::Direction, Kripke::Group, Kripke::Zone) (instance 1)]>, camp::list<RAJA::statement::For<(long)3, RAJA::policy::cuda::cuda_indexer<RAJA::iteration_mapping::StridedLoop<(unsigned long)0>, (RAJA::kernel_sync_requirement)0, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)-1, (int)0>>, RAJA::statement::For<(long)2, RAJA::policy::cuda::cuda_indexer<RAJA::iteration_mapping::StridedLoop<(unsigned long)0>, (RAJA::kernel_sync_requirement)0, RAJA::cuda::IndexGlobal<(RAJA::named_dim)1, (int)-1, (int)0>>, RAJA::statement::For<(long)0, RAJA::policy::cuda::cuda_indexer<RAJA::iteration_mapping::StridedLoop<(unsigned long)0>, (RAJA::kernel_sync_requirement)0, RAJA::cuda::IndexGlobal<(RAJA::named_dim)0, (int)0, (int)-1>>, RAJA::statement::For<(long)1, RAJA::policy::sequential::seq_exec, RAJA::statement::Lambda<(long)0, >>>>>>, RAJA::internal::LoopTypes<camp::list<void, void, void, void>, camp::list<void, void, void, void>>>>(T3)",
        debug=False,
    )
    assert kernel_str is None
    assert instance_num == "1"
    assert instance_exists is True
    assert skip_kernel is False


def test_match_kernel_str_to_cali_kripke_unsafe_returns_all_kernel_candidates():
    node_set = [
        Node({"name": "main", "type": "function"}),
        Node({"name": "Solve", "type": "function"}),
        Node({"name": "void kernel one()", "type": "kernel"}),
        Node({"name": "void kernel two()", "type": "kernel"}),
    ]
    matched_nodes = _match_kernel_str_to_cali(
        node_set, kernel_str=None, instance_num="1", instance_exists=True
    )
    assert len(matched_nodes) == 2
    assert all(node.frame["type"] == "kernel" for node in matched_nodes)


def test_laghos_kernel_uses_similarity_fallback_after_safe_wrapper_match():
    (
        kernel_str,
        demangled_kernel_name,
        instance_num,
        instance_exists,
        skip_kernel,
    ) = _match_call_trace_regex(
        ["main"],
        "void mfem::CuKernel1D<mfem::SparseMatrix::BooleanMult(const mfem::Array<int> &, mfem::Array<int> &) const::[lambda(int) (instance 1)]>(int, T1)",
        debug=False,
    )
    assert kernel_str == "CuKernel1D"
    assert instance_num == "1"
    assert instance_exists is True
    assert skip_kernel is False

    node_set = [
        Node(
            {
                "name": "void mfem::CuKernel1D<mfem::Vector::operator=(double)::{lambda(int)#1}>(int, mfem::Vector::operator=(double)::{lambda(int)#1})",
                "type": "kernel",
            }
        ),
        Node(
            {
                "name": "void mfem::CuKernel1D<mfem::SparseMatrix::BooleanMult(mfem::Array<int> const&, mfem::Array<int>&) const::{lambda(int)#1}>(int, mfem::SparseMatrix::BooleanMult(mfem::Array<int> const&, mfem::Array<int>&) const::{lambda(int)#1})",
                "type": "kernel",
            }
        ),
    ]
    matched_nodes = _match_kernel_str_to_cali(
        node_set, kernel_str, instance_num, instance_exists
    )
    assert len(matched_nodes) == 2

    matched_node = _multi_match_fallback_similarity(
        matched_nodes, demangled_kernel_name, debug=False
    )
    assert (
        matched_node.frame["name"]
        == "void mfem::CuKernel1D<mfem::SparseMatrix::BooleanMult(mfem::Array<int> const&, mfem::Array<int>&) const::{lambda(int)#1}>(int, mfem::SparseMatrix::BooleanMult(mfem::Array<int> const&, mfem::Array<int>&) const::{lambda(int)#1})"
    )
