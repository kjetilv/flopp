package com.github.kjetilv.flopp.files;

import com.github.kjetilv.flopp.Partition;
import com.github.kjetilv.flopp.Transfer;
import com.github.kjetilv.flopp.Transfers;

import java.nio.file.Path;

public final class FileChannelTransfers extends FileChannelBase implements Transfers<Path> {

    public FileChannelTransfers(Path target) {
        super(target, true);
    }

    @Override
    public Transfer transfer(Partition partition, Path result) {
        return new FileChannelTransfer(channel(), partition, result);
    }
}
