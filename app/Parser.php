<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use function array_count_values;
use function array_fill;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function json_encode;
use function ksort;
use function max;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
	private const int BLOG_PREFIX_LENGTH = 25;
	private const int WORKERS = 12;
	private const int CHUNK_SIZE = 1_048_576;
	private const int SMALL_FILE_THRESHOLD = 16_777_216;

	private const string FORMAT_16BIT = "\x01";
	private const string FORMAT_32BIT = "\x02";

	public function parse(string $inputPath, string $outputPath): void
	{
		gc_disable();

		$fileSize = filesize($inputPath);

		if (!$fileSize) {
			$outputStream = fopen($outputPath, "wb");
			fwrite($outputStream, "{}");
			fclose($outputStream);

			return;
		}

		[
			$packedDateIdByDate,
			$dateLabelById,
			$dateCount,
		] = $this->buildDateLookup();

		if (
			!$this->supportsFastDateRange(
				$inputPath,
				$fileSize,
				$packedDateIdByDate,
			)
		) {
			$this->parseGeneric($inputPath, $outputPath);

			return;
		}

		[$slugIdByPath, $slugPathById, $slugCount] = $this->buildSlugLookup(
			$inputPath,
			$fileSize,
		);

		$workerCount =
			$fileSize < self::SMALL_FILE_THRESHOLD ? 1 : self::WORKERS;

		$chunkBounds = [0];
		$inputStream = fopen($inputPath, "rb");

		for ($workerIndex = 1; $workerIndex < $workerCount; $workerIndex++) {
			fseek(
				$inputStream,
				(int) (($fileSize * $workerIndex) / $workerCount),
			);
			fgets($inputStream);
			$chunkBounds[] = ftell($inputStream);
		}

		$chunkBounds[] = $fileSize;
		fclose($inputStream);

		$chunkCount = count($chunkBounds) - 1;

		$temporaryDirectory = sys_get_temp_dir();
		$parentProcessId = getmypid();
		$childProcessFileByPid = [];

		for (
			$workerChunkIndex = 0;
			$workerChunkIndex < $chunkCount - 1;
			$workerChunkIndex++
		) {
			$temporaryFilePath =
				$temporaryDirectory .
				"/parse_" .
				$parentProcessId .
				"_" .
				$workerChunkIndex;
			$childProcessId = pcntl_fork();

			if ($childProcessId === 0) {
				$chunkCounts = $this->crunch(
					$inputPath,
					$chunkBounds[$workerChunkIndex],
					$chunkBounds[$workerChunkIndex + 1],
					$slugIdByPath,
					$packedDateIdByDate,
					$slugCount,
					$dateCount,
				);

				$uses16BitPacking = max($chunkCounts) <= 0xffff;
				$packedChunkCounts = $uses16BitPacking
					? pack("v*", ...$chunkCounts)
					: pack("V*", ...$chunkCounts);

				$temporaryFileStream = fopen($temporaryFilePath, "wb");
				fwrite(
					$temporaryFileStream,
					$uses16BitPacking ? self::FORMAT_16BIT : self::FORMAT_32BIT,
				);
				fwrite($temporaryFileStream, $packedChunkCounts);
				fclose($temporaryFileStream);

				exit(0);
			}

			$childProcessFileByPid[$childProcessId] = $temporaryFilePath;
		}

		$totalCounts = $this->crunch(
			$inputPath,
			$chunkBounds[$chunkCount - 1],
			$chunkBounds[$chunkCount],
			$slugIdByPath,
			$packedDateIdByDate,
			$slugCount,
			$dateCount,
		);

		$pendingChildProcessCount = count($childProcessFileByPid);
		while ($pendingChildProcessCount > 0) {
			$childExitStatus = 0;
			$childProcessId = pcntl_wait($childExitStatus);

			if ($childProcessId <= 0) {
				break;
			}

			if (!isset($childProcessFileByPid[$childProcessId])) {
				continue;
			}

			$encodedChildCounts = file_get_contents(
				$childProcessFileByPid[$childProcessId],
			);

			if ($encodedChildCounts !== false && $encodedChildCounts !== "") {
				$encodedFormat = $encodedChildCounts[0] ?? self::FORMAT_32BIT;
				$decodedCounts = unpack(
					$encodedFormat === self::FORMAT_16BIT ? "v*" : "V*",
					$encodedChildCounts,
					1,
				);

				$countIndex = 0;
				foreach ($decodedCounts as $decodedCount) {
					$totalCounts[$countIndex++] += $decodedCount;
				}
			}

			unlink($childProcessFileByPid[$childProcessId]);
			$pendingChildProcessCount--;
		}

		$outputStream = fopen($outputPath, "wb");
		stream_set_write_buffer($outputStream, 1_048_576);

		$datePrefixById = [];
		for ($dateId = 0; $dateId < $dateCount; $dateId++) {
			$datePrefixById[$dateId] =
				'        "' . $dateLabelById[$dateId] . '": ';
		}

		$escapedPathBySlugId = [];
		for ($slugId = 0; $slugId < $slugCount; $slugId++) {
			$escapedPathBySlugId[$slugId] =
				'"\\/blog\\/' .
				str_replace("/", "\\/", $slugPathById[$slugId]) .
				'"';
		}

		fwrite($outputStream, "{");
		$isFirstSlug = true;

		for ($slugId = 0; $slugId < $slugCount; $slugId++) {
			$slugOffset = $slugId * $dateCount;
			$dateLinesBuffer = "";
			$lineSeparator = "";

			for ($dateId = 0; $dateId < $dateCount; $dateId++) {
				$visitCount = $totalCounts[$slugOffset + $dateId];
				if ($visitCount === 0) {
					continue;
				}

				$dateLinesBuffer .=
					$lineSeparator . $datePrefixById[$dateId] . $visitCount;
				$lineSeparator = ",\n";
			}

			if ($dateLinesBuffer === "") {
				continue;
			}

			fwrite(
				$outputStream,
				($isFirstSlug ? "" : ",") .
					"\n    " .
					$escapedPathBySlugId[$slugId] .
					": {\n" .
					$dateLinesBuffer .
					"\n    }",
			);

			$isFirstSlug = false;
		}

		fwrite($outputStream, $isFirstSlug ? "}" : "\n}");
		fclose($outputStream);
	}

	private function crunch(
		string $inputPath,
		int $fromOffset,
		int $untilOffset,
		array $slugIdByPath,
		array $packedDateIdByDate,
		int $slugCount,
		int $dateCount,
	): array {
		$packedDateIdsBySlug = array_fill(0, $slugCount, "");

		$inputStream = fopen($inputPath, "rb");
		stream_set_read_buffer($inputStream, 0);
		fseek($inputStream, $fromOffset);

		$remainingBytes = $untilOffset - $fromOffset;

		while ($remainingBytes > 0) {
			$chunkBuffer = fread(
				$inputStream,
				$remainingBytes > self::CHUNK_SIZE
					? self::CHUNK_SIZE
					: $remainingBytes,
			);
			$length = strlen($chunkBuffer);

			if ($length === 0) {
				break;
			}

			$remainingBytes -= $length;

			$lastNewLineOffset = strrpos($chunkBuffer, "\n");
			if ($lastNewLineOffset === false) {
				break;
			}

			$tailLength = $length - $lastNewLineOffset - 1;
			if ($tailLength > 0) {
				fseek($inputStream, -$tailLength, SEEK_CUR);
				$remainingBytes += $tailLength;
			}

			$lineStartOffset = 0;
			$unrolledLoopLimit = $lastNewLineOffset - 480;

			while ($lineStartOffset < $unrolledLoopLimit) {
				$newLineOffset = strpos(
					$chunkBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				$slugPath = substr(
					$chunkBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);
				$dateKey = substr($chunkBuffer, $newLineOffset - 23, 8);
				$packedDateIdsBySlug[$slugIdByPath[$slugPath]] .=
					$packedDateIdByDate[$dateKey];
				$lineStartOffset = $newLineOffset + 1;

				$newLineOffset = strpos(
					$chunkBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				$slugPath = substr(
					$chunkBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);
				$dateKey = substr($chunkBuffer, $newLineOffset - 23, 8);
				$packedDateIdsBySlug[$slugIdByPath[$slugPath]] .=
					$packedDateIdByDate[$dateKey];
				$lineStartOffset = $newLineOffset + 1;

				$newLineOffset = strpos(
					$chunkBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				$slugPath = substr(
					$chunkBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);
				$dateKey = substr($chunkBuffer, $newLineOffset - 23, 8);
				$packedDateIdsBySlug[$slugIdByPath[$slugPath]] .=
					$packedDateIdByDate[$dateKey];
				$lineStartOffset = $newLineOffset + 1;

				$newLineOffset = strpos(
					$chunkBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				$slugPath = substr(
					$chunkBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);
				$dateKey = substr($chunkBuffer, $newLineOffset - 23, 8);
				$packedDateIdsBySlug[$slugIdByPath[$slugPath]] .=
					$packedDateIdByDate[$dateKey];
				$lineStartOffset = $newLineOffset + 1;
			}

			while ($lineStartOffset < $lastNewLineOffset) {
				$newLineOffset = strpos(
					$chunkBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				if ($newLineOffset === false) {
					break;
				}

				$slugPath = substr(
					$chunkBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);
				$dateKey = substr($chunkBuffer, $newLineOffset - 23, 8);
				$packedDateIdsBySlug[$slugIdByPath[$slugPath]] .=
					$packedDateIdByDate[$dateKey];
				$lineStartOffset = $newLineOffset + 1;
			}
		}

		fclose($inputStream);

		$flatCounts = array_fill(0, $slugCount * $dateCount, 0);

		for ($slugId = 0; $slugId < $slugCount; $slugId++) {
			if ($packedDateIdsBySlug[$slugId] === "") {
				continue;
			}

			$slugOffset = $slugId * $dateCount;

			foreach (
				array_count_values(unpack("v*", $packedDateIdsBySlug[$slugId]))
				as $dateId => $visitCount
			) {
				$flatCounts[$slugOffset + $dateId] += $visitCount;
			}
		}

		return $flatCounts;
	}

	private function buildDateLookup(): array
	{
		$dateIdByDate = [];
		$dateLabelById = [];
		$dateCount = 0;

		for ($yearTwoDigits = 20; $yearTwoDigits <= 26; $yearTwoDigits++) {
			for ($monthNumber = 1; $monthNumber <= 12; $monthNumber++) {
				$daysInMonth = match ($monthNumber) {
					2 => $yearTwoDigits % 4 === 0 ? 29 : 28,
					4, 6, 9, 11 => 30,
					default => 31,
				};

				$monthString = ($monthNumber < 10 ? "0" : "") . $monthNumber;
				$yearMonthPrefix = $yearTwoDigits . "-" . $monthString . "-";

				for ($dayNumber = 1; $dayNumber <= $daysInMonth; $dayNumber++) {
					$dateKey =
						$yearMonthPrefix .
						(($dayNumber < 10 ? "0" : "") . $dayNumber);
					$dateIdByDate[$dateKey] = $dateCount;
					$dateLabelById[$dateCount] = "20" . $dateKey;
					$dateCount++;
				}
			}
		}

		$packedDateIdByDate = [];
		foreach ($dateIdByDate as $dateKey => $dateId) {
			$packedDateIdByDate[$dateKey] =
				chr($dateId & 0xff) . chr($dateId >> 8);
		}

		return [$packedDateIdByDate, $dateLabelById, $dateCount];
	}

	private function supportsFastDateRange(
		string $inputPath,
		int $fileSize,
		array $packedDateIdByDate,
	): bool {
		$inputStream = fopen($inputPath, "rb");
		stream_set_read_buffer($inputStream, 0);
		$sampleBuffer = fread(
			$inputStream,
			$fileSize > 2_097_152 ? 2_097_152 : $fileSize,
		);
		fclose($inputStream);

		$sampleLastNewLineOffset = strrpos($sampleBuffer, "\n");
		if ($sampleLastNewLineOffset === false) {
			return true;
		}

		$lineStartOffset = 0;

		while ($lineStartOffset < $sampleLastNewLineOffset) {
			$newLineOffset = strpos($sampleBuffer, "\n", $lineStartOffset + 53);
			if ($newLineOffset === false) {
				break;
			}

			$dateKey = substr($sampleBuffer, $newLineOffset - 23, 8);
			if (!isset($packedDateIdByDate[$dateKey])) {
				return false;
			}

			$lineStartOffset = $newLineOffset + 1;
		}

		return true;
	}

	private function parseGeneric(string $inputPath, string $outputPath): void
	{
		$visitsByPathAndDate = [];

		$inputStream = fopen($inputPath, "rb");
		stream_set_read_buffer($inputStream, 0);

		while (($line = fgets($inputStream)) !== false) {
			$commaOffset = strpos($line, ",", self::BLOG_PREFIX_LENGTH);
			if ($commaOffset === false) {
				continue;
			}

			$path = substr(
				$line,
				self::BLOG_PREFIX_LENGTH,
				$commaOffset - self::BLOG_PREFIX_LENGTH,
			);
			$date = substr($line, $commaOffset + 1, 10);

			$visitsByPathAndDate[$path][$date] =
				($visitsByPathAndDate[$path][$date] ?? 0) + 1;
		}

		fclose($inputStream);

		foreach ($visitsByPathAndDate as &$visitsByDate) {
			ksort($visitsByDate);
		}

		unset($visitsByDate);

		$outputStream = fopen($outputPath, "wb");
		fwrite(
			$outputStream,
			json_encode($visitsByPathAndDate, JSON_PRETTY_PRINT) ?: "{}",
		);
		fclose($outputStream);
	}

	private function buildSlugLookup(string $inputPath, int $fileSize): array
	{
		$inputStream = fopen($inputPath, "rb");
		stream_set_read_buffer($inputStream, 0);
		$sampleBuffer = fread(
			$inputStream,
			$fileSize > 2_097_152 ? 2_097_152 : $fileSize,
		);
		fclose($inputStream);

		$slugIdByPath = [];
		$slugPathById = [];
		$slugCount = 0;

		$sampleLastNewLineOffset = strrpos($sampleBuffer, "\n");
		if ($sampleLastNewLineOffset !== false) {
			$lineStartOffset = 0;

			while ($lineStartOffset < $sampleLastNewLineOffset) {
				$newLineOffset = strpos(
					$sampleBuffer,
					"\n",
					$lineStartOffset + 53,
				);
				if ($newLineOffset === false) {
					break;
				}

				$slugPath = substr(
					$sampleBuffer,
					$lineStartOffset + self::BLOG_PREFIX_LENGTH,
					$newLineOffset - $lineStartOffset - 51,
				);

				if (!isset($slugIdByPath[$slugPath])) {
					$slugIdByPath[$slugPath] = $slugCount;
					$slugPathById[$slugCount] = $slugPath;
					$slugCount++;
				}

				$lineStartOffset = $newLineOffset + 1;
			}
		}

		foreach (Visit::all() as $visit) {
			$slugPath = substr($visit->uri, self::BLOG_PREFIX_LENGTH);

			if (!isset($slugIdByPath[$slugPath])) {
				$slugIdByPath[$slugPath] = $slugCount;
				$slugPathById[$slugCount] = $slugPath;
				$slugCount++;
			}
		}

		return [$slugIdByPath, $slugPathById, $slugCount];
	}
}
