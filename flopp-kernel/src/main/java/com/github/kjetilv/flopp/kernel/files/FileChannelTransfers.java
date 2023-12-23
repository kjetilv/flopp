package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.nio.file.Path;

final class FileChannelTransfers extends AbstractFileChanneling implements Transfers<Path> {

    FileChannelTransfers(Path target) {
        super(target, true);
    }

    @Override
    public Transfer transfer(Partition partition, Path result) {
        return new FileChannelTransfer(channel(), partition, result);
    }
}
