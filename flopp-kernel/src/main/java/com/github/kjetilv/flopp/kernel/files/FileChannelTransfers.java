package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Transfer;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.nio.file.Path;

final class FileChannelTransfers extends AbstractFileChanneling implements Transfers<Path> {

    FileChannelTransfers(Path path) {
        super(path, true);
    }

    @Override
    public Transfer transfer(Partition partition, Path result) {
        return new FileChannelTransfer(randomAccess(true), partition, result);
    }
}
