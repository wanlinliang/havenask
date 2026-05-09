package(
    default_visibility=['//aios/storage/indexlib/index/ann:__subpackages__']
)
load('//aios/storage:defs.bzl', 'strict_cc_library')

config_setting(
    name='enable_ann_aitheta2',
    values={'define': 'enable_ann_aitheta2=true'}
)

strict_cc_library(
    name='config',
    srcs=['ANNIndexConfig.cpp'],
    hdrs=['ANNIndexConfig.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':constants',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=['Common.h'],
    visibility=['//visibility:public'],
    deps=[]
)
strict_cc_library(
    name='ANNPostingIterator',
    srcs=['ANNPostingIterator.cpp'],
    hdrs=['ANNPostingIterator.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':constants',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index:TermMatchData',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='factory',
    srcs=['ANNIndexFactory.cpp'],
    hdrs=['ANNIndexFactory.h'],
    copts=select({
        ':enable_ann_aitheta2': ['-DINDEXLIB_ENABLE_ANN_AITHETA2=1'],
        '//conditions:default': [],
    }),
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=(
        [':constants', '//aios/storage/indexlib/index:IIndexFactory'] +
        select({
            ':enable_ann_aitheta2': [
                '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_disk_indexer',
                '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_merger',
                '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_reader',
                '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_mem_indexer'
            ],
            '//conditions:default': [],
        })
    ),
    alwayslink=True
)
