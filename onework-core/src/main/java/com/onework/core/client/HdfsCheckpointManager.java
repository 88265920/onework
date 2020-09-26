package com.onework.core.client;

import com.onework.core.conf.OneWorkConf;
import com.onework.core.utils.HadoopXmlUtils;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.dom4j.DocumentException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
@Component
public class HdfsCheckpointManager {
    private OneWorkConf oneWorkConf;
    private FileSystem fs;
    private Map<String, String> hdfsConfigs = new HashMap<>();

    @Autowired
    public HdfsCheckpointManager(OneWorkConf oneWorkConf) throws DocumentException {
        this.oneWorkConf = oneWorkConf;
        hdfsConfigs.putAll(HadoopXmlUtils.readXML(oneWorkConf.getHdfs().getCoreSiteFile()));
        hdfsConfigs.putAll(HadoopXmlUtils.readXML(oneWorkConf.getHdfs().getHdfsSiteFile()));
        checkState(hdfsConfigs.size() > 0);
    }

    @SneakyThrows
    private FileSystem createClient(Map<String, String> configs) {
        Configuration conf = new Configuration();

        String key = "fs.defaultFS";
        String fsAddr = configs.get(key);
        checkState(StringUtils.isNotEmpty(fsAddr));
        conf.set(key, fsAddr);

        key = "dfs.nameservices";
        String nameService = configs.get(key);
        // HA
        if (StringUtils.isNotEmpty(nameService)) {
            conf.set(key, nameService);

            key = "dfs.ha.namenodes.".concat(nameService);
            String nameNodes = configs.get(key);
            checkState(StringUtils.isNotEmpty(nameNodes));
            conf.set(key, nameNodes);

            for (String nameNode : nameNodes.split(",")) {
                key = "dfs.namenode.rpc-address." + nameService + nameNode;
                String value = configs.get(key);
                checkState(StringUtils.isNotEmpty(value));
                conf.set(key, value);
            }

            key = "dfs.client.failover.proxy.provider." + nameService;
            String proxyProvider = configs.get(key);
            checkState(StringUtils.isNotEmpty(proxyProvider));
            conf.set(key, proxyProvider);
        }

        String fileOwner = oneWorkConf.getHdfs().getFileOwner();
        checkState(StringUtils.isNotEmpty(fileOwner));

        return FileSystem.get(new URI(fsAddr), conf, fileOwner);
    }

    @SneakyThrows
    public String getLatestCheckPoint(@NonNull String jobId) {
        String basePath = oneWorkConf.getFlink().getCheckpoint() + '/' + jobId;
        RemoteIterator<LocatedFileStatus> filesIterator = getFs().listFiles(new Path(basePath), true);
        String latestPath = null;
        int latestCheckPointNum = 0;
        while (filesIterator.hasNext()) {
            Path p = filesIterator.next().getPath();
            if (p.getName().equals("_metadata")) {
                String chkPath = p.getParent().getName();
                int checkPointNum = Integer.parseInt(chkPath.substring(chkPath.indexOf('-') + 1));
                if (checkPointNum > latestCheckPointNum) {
                    latestCheckPointNum = checkPointNum;
                    latestPath = basePath + '/' + chkPath;
                }
            }
        }
        return latestPath;
    }

    @SneakyThrows
    public boolean hasCheckPoint(@NonNull String jobId) {
        String basePath = oneWorkConf.getFlink().getCheckpoint() + '/' + jobId;
        RemoteIterator<LocatedFileStatus> filesIterator = getFs().listFiles(new Path(basePath), true);
        while (filesIterator.hasNext()) {
            Path p = filesIterator.next().getPath();
            if (p.getName().equals("_metadata")) {
                return true;
            }
        }
        return false;
    }

    @SneakyThrows
    public void clearExpiredCheckpoint(@Nonnull Set<String> jobIds) {
        Set<String> usedFiles = new HashSet<>();
        for (String jobId : jobIds) {
            usedFiles.addAll(getUsedCheckpointFiles(jobId));
        }
        List<String> checkpointJobIds = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> filesIterator = getFs().listLocatedStatus(
                new Path(oneWorkConf.getFlink().getCheckpoint()));
        while (filesIterator.hasNext()) {
            checkpointJobIds.add(filesIterator.next().getPath().getName());
        }
        for (String jobId : checkpointJobIds) {
            if (jobIds.contains(jobId)) continue;
            String checkpoint = oneWorkConf.getFlink().getCheckpoint() + '/' + jobId;
            filesIterator = getFs().listFiles(new Path(checkpoint.concat("/shared")), true);
            boolean deleteDir = true;
            while (filesIterator.hasNext()) {
                LocatedFileStatus fileStatus = filesIterator.next();
                if (usedFiles.contains(fileStatus.getPath().getName())) {
                    deleteDir = false;
                }
            }
            if (deleteDir) {
                fs.delete(new Path(checkpoint), true);
                log.info("Clear expired checkpoint directory: {}", checkpoint);
            }
        }
    }

    @SneakyThrows
    private Set<String> getUsedCheckpointFiles(@Nonnull String jobId) {
        Set<String> usedCheckpointFiles = new HashSet<>();
        String metadataPath = getLatestCheckPoint(jobId) + "/_metadata";
        String localMetadataPath = "file:///tmp/_metadata";
        fs.copyToLocalFile(new Path(metadataPath), new Path(localMetadataPath));
        CompletedCheckpointStorageLocation location = AbstractFsCheckpointStorage.resolveCheckpointPointer(localMetadataPath);
        try (DataInputStream stream = new DataInputStream(location.getMetadataHandle().openInputStream())) {
            CheckpointMetadata metadata = Checkpoints.loadCheckpointMetadata(stream,
                    Thread.currentThread().getContextClassLoader(), localMetadataPath);
            for (OperatorState operatorState : metadata.getOperatorStates()) {
                if (operatorState.getStateSize() == 0) {
                    continue;
                }
                for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                    for (KeyedStateHandle keyedStateHandle : operatorSubtaskState.getManagedKeyedState()) {
                        if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                            Map<StateHandleID, StreamStateHandle> sharedState =
                                    ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
                            for (Map.Entry<StateHandleID, StreamStateHandle> entry : sharedState.entrySet()) {
                                if (entry.getValue() instanceof FileStateHandle) {
                                    org.apache.flink.core.fs.Path filePath = ((FileStateHandle) entry.getValue()).getFilePath();
                                    usedCheckpointFiles.add(filePath.getName());
                                }
                            }
                        }
                    }
                }
            }
        }
        return usedCheckpointFiles;
    }

    @SneakyThrows
    public FileSystem getFs() {
        if (fs == null) {
            fs = createClient(hdfsConfigs);
        }
        return fs;
    }

    @PreDestroy
    public void close() throws IOException {
        if (Objects.nonNull(fs)) {
            fs.close();
        }
    }
}
