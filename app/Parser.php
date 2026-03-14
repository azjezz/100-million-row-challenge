<?php

declare(strict_types=1);

namespace App;

use function array_fill;
use function asort;
use function fclose;
use function feof;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;

final readonly class Parser
{
    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $workers = 16;
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ($fileSize / $workers);

        $safeSkip = 55;
        if (($fileSize % 100_000_000) === 0) {
            $safeSkip = (int) ($fileSize / 100_000_000) - 1;
        }

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);
        $boundaries[] = $fileSize;
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > 4_194_304 ? 4_194_304 : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);
        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $dateIds = [];
        $dateIds7 = [];
        $dates = [];
        $dateCount = 0;
        $warmUpCounts = [];
        $pos = 0;
        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + $safeSkip);
            $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
            $pathId = $pathIds[$path] ?? $pathCount;

            if ($pathId === $pathCount) {
                $pathIds[$path] = $pathId;
                $paths[$pathCount] = $path;
                $pathCount++;
            }

            $date = substr($chunk, $nlPos - 25, 10);
            $dateId = $dateIds[$date] ?? -1;

            if ($dateId === -1) {
                $dateId = $dateCount;
                $dateIds[$date] = $dateId;
                $dateIds7[substr($date, 3, 7)] = $dateId;
                $dates[$dateCount] = $date;
                $dateCount++;
            }

            $warmUpCounts[$pathId][$dateId] = ($warmUpCounts[$pathId][$dateId] ?? 0) + 1;
            $pos = $nlPos + 1;
        }

        unset($chunk);
        $warmUpEnd = $lastNl + 1;
        for ($i = 0; $i < $workers; $i++) {
            if ($boundaries[$i] < $warmUpEnd) {
                $boundaries[$i] = $warmUpEnd;
            }
        }

        $stride = $dateCount;

        $pathBases = [];
        foreach ($pathIds as $path => $id) {
            $pathBases[$path] = $id * $stride;
        }

        $fast = [];
        $conflict = [];
        foreach ($pathBases as $slug => $base) {
            $l = strlen($slug);
            $f = $slug[0];
            $la = $slug[$l - 1];
            if (isset($conflict[$l][$f][$la])) {
            } elseif (isset($fast[$l][$f][$la])) {
                $conflict[$l][$f][$la] = true;
                unset($fast[$l][$f][$la]);
            } else {
                $fast[$l][$f][$la] = $base;
            }
        }
        unset($conflict);

        $total = $pathCount * $stride;
        $outputSize = $total;

        $counts = array_fill(0, $total, 0);
        foreach ($warmUpCounts as $pId => $dateCounts) {
            $base = $pId * $stride;
            foreach ($dateCounts as $dId => $count) {
                $counts[$base + $dId] = $count;
            }
        }
        unset($warmUpCounts);

        $sockets = [];
        for ($w = 0; $w < $workers; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            if (pcntl_fork() === 0) {
                fclose($pair[0]);

                // Inlined processChunk
                $cnts = str_repeat("\0", $outputSize);
                $inc = [];
                for ($i = 0; $i < 255; $i++) {
                    $inc[chr($i)] = chr($i + 1);
                }

                $h = fopen($inputPath, 'rb');
                stream_set_read_buffer($h, 0);
                fseek($h, $boundaries[$w]);

                $remaining = $boundaries[$w + 1] - $boundaries[$w];

                while ($remaining > 0) {
                    $chunk = fread($h, $remaining > 33_554_432 ? 33_554_432 : $remaining);
                    $chunkLen = strlen($chunk);
                    $remaining -= $chunkLen;

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl < ($chunkLen - 1)) {
                        $excess = $chunkLen - $lastNl - 1;
                        fseek($h, -$excess, SEEK_CUR);
                        $remaining += $excess;
                    }

                    $p = 25;
                    $limit = $lastNl + 25;

                    while ($p < $limit) {
                        $c = strpos($chunk, ',', $p);
                        $idx = ($fast[$c - $p][$chunk[$p]][$chunk[$c - 1]] ?? $pathBases[substr($chunk, $p, $c - $p)])
                            + $dateIds7[substr($chunk, $c + 4, 7)];
                        $cnts[$idx] = $inc[$cnts[$idx]];
                        $p = $c + 52;
                    }
                }

                fclose($h);
                fwrite($pair[1], $cnts);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $socketOffsets = array_fill(0, $workers, 0);
        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $outputSize);
                $dataLen = strlen($data);
                $offset = $socketOffsets[$key];

                $alignedLen = $dataLen & ~3;
                if ($alignedLen > 0) {
                    foreach (unpack('V*', substr($data, 0, $alignedLen)) as $v) {
                        $counts[$offset] += $v & 0xFF;
                        $counts[$offset + 1] += ($v >> 8) & 0xFF;
                        $counts[$offset + 2] += ($v >> 16) & 0xFF;
                        $counts[$offset + 3] += ($v >> 24) & 0xFF;
                        $offset += 4;
                    }
                }

                for ($r = $alignedLen; $r < $dataLen; $r++) {
                    $counts[$offset] += ord($data[$r]);
                    $offset++;
                }

                $socketOffsets[$key] = $offset;
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                }
            }
        }

        while (pcntl_waitpid(-1, $status) > 0) {
        }

        $sortedDates = $dates;
        asort($sortedDates);
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');
        $firstPath = true;
        foreach ($paths as $pathId => $path) {
            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $pathBuffer .= "\n    \"\/blog\/{$path}\": {";
            $entries = [];
            $base = $pathId * $stride;

            foreach ($sortedDates as $dateId => $dateStr) {
                $count = $counts[$base + $dateId];
                if ($count === 0) {
                    continue;
                }

                $entries[] = "        \"{$dateStr}\": {$count}";
            }

            $pathBuffer .= "\n" . implode(",\n", $entries) . "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
