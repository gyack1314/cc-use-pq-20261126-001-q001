# Strata Vault · 地层档案库

现场记录可以在离线工作站分片制作成档案包，运抵接收站后按所缺分片续传。接收站校验完整档案，再以不可变版本发布，同一字节对象可供多个版本共享。报告引用可以固定具体版本。库房发现介质损坏时可以查出受影响的版本，输入经验证的字节完成修复。示例和测试均使用本地合成记录。

## 运行

使用 Python 3.11 或更高版本，并预装 `requirements.txt` 中的版本。将库目录置于本机可靠的文件系统上。服务默认绑定环回地址，供受信任的接收站与维护人员使用。目录应由服务独占写入。

```bash
python -m strata --data vault-data
python -m strata.field --help
python -m unittest discover -s tests -v
python -m compileall -q strata
```

服务启动后可通过 `/docs` 查看接口、请求结构和参数。所有时刻和期限均使用整数秒，时刻基于 Unix 时间。

## 入库与版本

先向 `POST /collections` 提交 `name` 与 `retention_seconds` 创建馆藏。馆藏初始头版本为 0。现场用 `python -m strata.field capture` 将正常文件目录封装成可搬运的本地档案包。捕获参数包含馆藏名、源端令牌和它所依据的头版本。原目录请保持静止直至捕获完成。可用 `check` 校验现场档案，可在接收站用 `submit` 上传。弱网导致响应丢失时，用同一个档案包重新提交会回到同一会话并查询缺片。

`POST /collections/{collection}/deposits` 接收 `token`、`parent`、`lifetime_seconds` 及 `manifest`。清单版本为 `1`，每个文件包含 `path`、`size`、`sha256` 和按原文件顺序排列的 `parts`。分片由 `sha256` 与 `size` 描述。清单按路径排序后生成 UTF-8 JSON 指纹，文件内分片顺序参与指纹。空文件可以入库。档案最多 500 个文件、8192 个分片引用、4 GiB 逻辑字节，单片最多 16 MiB。文件路径须在不同操作系统上保持唯一，保留 `_vault` 给出库元数据。

逐片以 `application/octet-stream` 上送到 `PUT /deposits/{id}/parts/{digest}`。重复上传保留已有收据，共享字节只落盘一次。将 `POST /deposits/{id}/publish` 作为最后一步。系统会核对全文件摘要，并比较当前头版本与提交时依据的版本。图层两次分别基于同一头版本送审时，只有一次可以成为下一版。发布回执丢失可再次请求同一会话。取消及到期的会话保留原令牌和清单痕迹。

## 取回与维护

每个历史版本可按馆藏、版本号和清单摘要引用。可用 `GET /collections/{collection}/revisions/{number}/file?path=…` 取回单文件，支持单个字节范围。服务先核对组成该文件的全部字节，再发送成功响应。完整 ZIP 可通过同一路径下的 `package` 取回，附 `_vault/manifest.json` 和 `_vault/receipt.json`。`python -m strata.field verify` 需要已知的馆藏、版本和清单摘要，支持在验证成功后还原到新目录。外部传来的 ZIP 应使用已可信保存的引用核对。

- `coverage` 显示各文件受已知介质问题的影响。取回未涉及损坏对象的文件仍可继续。
- `holds/{reference}` 支持固定与解除版本。头版本、仍在保留期的版本和引用未解除的版本会阻止退役，退役保留版本清单和事件。
- `/operations/scan` 分页重新读取介质。事件编号用于把修复请求绑定到仍有效的损坏记录。一次修复会恢复该对象被多个版本共享的字节。
- `/operations/forecast` 与馆藏的 `footprint` 分别显示全库待回收对象和馆藏的保护占用。先执行 `expire` 释放已到期会话的收据，再用 `mark` 和 `sweep` 回收。已标记对象会暂停接收新收据，回收进程重启后可继续。
- `/operations/reconcile` 可核对未入目录的物理文件，默认只读。`/operations/staging-cleanup` 清理中断写入的临时文件。
- `/operations/audit` 检查目录中的版本链、保留、清单和收据关系。介质字节另由扫描验证。`/events` 可查库级事件或按馆藏过滤。

## 分卷交割

容量受限或分批到达的载体可以用分卷交割送出已发布版本。馆员选定馆藏与版本，给出每卷最大字节数：

```bash
curl -X POST http://127.0.0.1:8080/shipments \
  -H 'Content-Type: application/json' \
  -d '{"collection":"ridge","revision":1,"max_volume_bytes":4194304}'
```

系统把该版本的逻辑字节流（按清单路径排序的文件顺序拼接）确定性地切成定长卷，因此单个大文件可以跨任意多卷。返回的 `id` 是交割编号，`credential`/`token` 是收货凭据；`volumes[]` 给出每卷的 `number`（1 基，另附 0 基 `index`）、`start`/`size` 与 `sha256`。同一交割重寄时这些编号和摘要保持不变。卷字节由 `GET /shipments/{id}/volumes/{number}` 按需从介质流式校验并送出，发送端只缓冲当前一卷，不持久化整组卷。`/handovers`、`/consignments`、`/dispatches`、`/deliveries` 是同义资源前缀，也支持 `/collections/{c}/revisions/{n}/shipments` 嵌套写法。

收货方可用 `GET /shipments/{id}/ticket` 取得含原版本清单的可信票据（JSON），连同载体目录在断网现场核对：

```bash
python -m strata.field volumes ticket.json --dir /media/carriers --extract ./restored
```

也可在代码中使用 `strata.field.shipment.verify_volumes(ticket, directory)`（别名 `verify_shipment`/`check_volumes`，亦可从 `strata.field.receive` 导入）。报告区分 `missing`、`invalid`（摘要不符）与已验证卷，并列出当前已能完整取回的 `complete_files`/`recoverable_files`；提供 `destination` 时仅在全套卷正确后按原清单逐文件校验并原子还原。载体文件可按 `volume-001.bin` 等命名，也可用卷摘要或任意文件名（按内容摘要匹配），或直接传入 `{卷号: 字节/路径}` 映射。

接收站用 `POST /shipments/{id}/volumes/{number}/ack`（或 `/confirm`、`/sign`、直接 POST 该卷资源、顶层 `/receipts`）回报签收，可在 JSON 中带 `credential` 与接收方算出的 `sha256`。签收按卷幂等成行：重复回报、丢失回复后重试都返回同一状态，单卷摘要不符不会删除其他卷的有效签收。状态中 `sent_volumes`/`dispatched_volumes` 是档案库已发出（取过字节或登记过寄送）的卷，`acknowledged_volumes`/`confirmed_volumes` 是实际签收的卷，二者分别列出。

登记交割会写入一条 `shipment-{id}` 保留固定，使版本即使超过 `retention_seconds` 也无法退役，保证最后一个未确认卷仍可补寄。全部卷签收（状态转为 `completed`）或 `DELETE /shipments/{id}` / `.../revoke` 明确撤销后，该固定立即释放，版本可照常退役回收；各卷签收证据与事件仍保留，`GET /shipments/{id}/receipts` 可查。

大文件取回时，服务在同一次目录事务中核验并准备响应，完成后释放写锁并送出数据。准备过程会占用本机暂存空间。业务繁忙时客户端可在收到临时繁忙响应后按原令牌重试。
